#!/usr/bin/env python3
"""
QuickBooks Online Bank Transaction Scraper - HTTP API Version

Uses direct API calls instead of Playwright for scraping.
Playwright is only needed for initial login to get session cookies.

API Endpoints discovered:
    GET /api/neo/v1/company/{companyId}/olb/ng/getInitialData
        → Returns list of connected bank accounts
    
    GET /api/neo/v1/company/{companyId}/olb/ng/getTransactions
        ?accountId={id}&sort=-txnDate&reviewState=PENDING&ignoreMatching=false
        → Returns pending transactions for an account

Usage:
    python qb_http_scraper.py --login       # One-time: login via Playwright, save session
    python qb_http_scraper.py --scrape      # Scrape via HTTP API (no browser needed)
    python qb_http_scraper.py --sync        # Scrape and sync to QuickBase
"""

import os
import sys
import json
import argparse
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict, field
from http.cookies import SimpleCookie
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

# =============================================================================
# Configuration
# =============================================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
DATA_DIR = Path(os.getenv('QB_DATA_DIR', Path.home() / '.qb_bank_scraper'))
SESSION_DIR = Path(os.getenv('QB_SESSION_DIR', DATA_DIR / 'session'))
STATE_FILE = SESSION_DIR / 'state.json'

# QuickBase config
QUICKBASE_REALM = os.getenv('QUICKBASE_REALM')
QUICKBASE_TOKEN = os.getenv('QUICKBASE_TOKEN')
QUICKBASE_TABLE_ID = os.getenv('QUICKBASE_TABLE_ID')

# QB API config (these are static/public values from their JS bundle)
QB_API_KEY = 'prdakyresxaDrhFXaSARXaUdj1S8M7h6YK7YGekc'
QB_BASE_URL = 'https://qbo.intuit.com'

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
SESSION_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class BankAccount:
    """Connected bank account from QB"""
    qbo_account_id: str
    name: str
    account_type: str
    balance: float
    pending_count: int = 0
    institution_name: str = ''
    mask: str = ''  # Last 4 digits


@dataclass 
class PendingTransaction:
    """Pending bank transaction from QB"""
    id: str
    olb_txn_id: str
    date: str
    description: str
    original_description: str
    amount: float
    transaction_type: str  # 'spent' or 'received'
    account_id: str
    account_name: str
    merchant_name: Optional[str] = None
    suggested_category: Optional[str] = None
    category_explanation: Optional[str] = None
    confidence: Optional[str] = None
    scraped_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    @property
    def unique_key(self) -> str:
        """Generate unique key for deduplication"""
        return f"{self.account_id}_{self.olb_txn_id}"


# =============================================================================
# Session Management
# =============================================================================

class QBSession:
    """Manages QuickBooks session from Playwright state.json"""
    
    def __init__(self, state_file: Path = STATE_FILE):
        self.state_file = state_file
        self.cookies: Dict[str, str] = {}
        self.company_id: Optional[str] = None
        self.user_id: Optional[str] = None
        self.csrf_token: Optional[str] = None
        self.x_csrf_token: Optional[str] = None
        self._load_session()
    
    def _load_session(self):
        """Load and parse Playwright session state"""
        if not self.state_file.exists():
            raise FileNotFoundError(f"Session file not found: {self.state_file}")
        
        with open(self.state_file, 'r') as f:
            state = json.load(f)
        
        # Extract cookies
        for cookie in state.get('cookies', []):
            if 'intuit.com' in cookie.get('domain', ''):
                self.cookies[cookie['name']] = cookie['value']
        
        # Extract key values from cookies
        self.company_id = self.cookies.get('qbo.currentcompanyid')
        self.user_id = self.cookies.get('qbo.authid') or self.cookies.get('userIdentifier')
        self.csrf_token = self.cookies.get('qbo.csrftoken')
        self.x_csrf_token = self.cookies.get('qbo.xcsrfderivationkey')
        
        if not self.company_id:
            raise ValueError("Could not extract company ID from session")
        
        logger.info(f"Session loaded: company={self.company_id}, user={self.user_id}")
    
    def get_cookie_header(self) -> str:
        """Build Cookie header string"""
        return '; '.join(f'{k}={v}' for k, v in self.cookies.items())
    
    def get_headers(self) -> Dict[str, str]:
        """Build request headers for QB API calls"""
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'apiKey': QB_API_KEY,
            'Authorization': f'Intuit_APIKey intuit_apikey={QB_API_KEY}, intuit_apikey_version=1.0',
            'authType': 'browser_auth',
            'Content-Type': 'application/json; charset=UTF-8',
            'Cookie': self.get_cookie_header(),
            'intuit-company-id': self.company_id,
            'intuit-user-id': self.user_id or '',
            'intuit-plugin-id': 'integrations-datain-ui',
            'Referer': f'{QB_BASE_URL}/app/banking',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
        }
        
        if self.csrf_token:
            headers['Csrftoken'] = self.csrf_token
        
        if self.x_csrf_token:
            headers['x-csrf-token'] = self.x_csrf_token
        
        return headers
    
    @property
    def is_valid(self) -> bool:
        """Check if session appears valid"""
        return bool(self.company_id and self.cookies.get('qbo.ticket'))


# =============================================================================
# QB HTTP API Client
# =============================================================================

class QBHttpClient:
    """HTTP client for QuickBooks API"""
    
    def __init__(self, session: QBSession):
        self.session = session
        self.base_url = QB_BASE_URL
        self._http = requests.Session()
        self._cookies_updated = False
        self._updated_cookies = {}
    
    def _make_request(self, method: str, endpoint: str, params: dict = None, 
                      headers: dict = None, **kwargs) -> requests.Response:
        """Make authenticated request to QB API"""
        url = f"{self.base_url}{endpoint}"
        
        req_headers = self.session.get_headers()
        if headers:
            req_headers.update(headers)
        
        response = self._http.request(
            method=method,
            url=url,
            params=params,
            headers=req_headers,
            **kwargs
        )
        
        # Check for Set-Cookie headers (session refresh)
        if 'Set-Cookie' in response.headers:
            self._cookies_updated = True
            # Capture any refreshed cookies
            for cookie in self._http.cookies:
                if 'intuit' in (cookie.domain or ''):
                    self._updated_cookies[cookie.name] = cookie.value
                    logger.debug(f"Cookie updated: {cookie.name}")
        
        return response
    
    @property
    def cookies_were_refreshed(self) -> bool:
        """Check if any cookies were refreshed during API calls"""
        return self._cookies_updated
    
    def get_updated_cookies(self) -> Dict[str, str]:
        """Get any cookies that were refreshed"""
        return self._updated_cookies
    
    def check_session(self) -> bool:
        """Verify session is still valid by making a test request"""
        try:
            response = self._make_request(
                'GET',
                f'/api/neo/v1/company/{self.session.company_id}/olb/ng/getInitialData'
            )
            
            if response.status_code == 200:
                data = response.json()
                # Check if we got actual account data
                if 'accounts' in data:
                    logger.info("Session valid - API responding")
                    return True
            
            if response.status_code in [401, 403]:
                logger.warning(f"Session invalid - got {response.status_code}")
                return False
            
            logger.warning(f"Unexpected response: {response.status_code}")
            return False
            
        except Exception as e:
            logger.error(f"Session check failed: {e}")
            return False
    
    def get_accounts(self) -> tuple[List[BankAccount], dict]:
        """Get list of connected bank accounts"""
        response = self._make_request(
            'GET',
            f'/api/neo/v1/company/{self.session.company_id}/olb/ng/getInitialData'
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to get accounts: {response.status_code} - {response.text[:500]}")
        
        data = response.json()
        accounts = []
        
        for acct in data.get('accounts', []):
            # Parse pending count from the nested structure
            pending_count = 0
            if 'pendingCount' in acct:
                pending_count = acct['pendingCount']
            
            account = BankAccount(
                qbo_account_id=str(acct.get('qboAccountId', '')),
                name=acct.get('qboAccountName', acct.get('name', 'Unknown')),
                account_type=acct.get('qboAccountType', ''),
                balance=float(acct.get('qboBalance', 0)),
                pending_count=pending_count,
                institution_name=acct.get('institutionName', ''),
                mask=acct.get('mask', '')
            )
            accounts.append(account)
        
        logger.info(f"Found {len(accounts)} connected accounts")
        return accounts, data
    
    def get_pending_transactions(self, account_id: str, account_name: str = '',
                                  limit: int = 200) -> List[PendingTransaction]:
        """Get pending/for-review transactions for an account"""
        transactions = []
        offset = 0
        page_size = 50
        
        while offset < limit:
            response = self._make_request(
                'GET',
                f'/api/neo/v1/company/{self.session.company_id}/olb/ng/getTransactions',
                params={
                    'accountId': account_id,
                    'sort': '-txnDate',
                    'reviewState': 'PENDING',
                    'ignoreMatching': 'false'
                },
                headers={
                    'X-Range': f'items={offset}-{offset + page_size - 1}'
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get transactions for account {account_id}: {response.status_code}")
                break
            
            data = response.json()
            items = data.get('items', [])
            
            if not items:
                break
            
            for item in items:
                txn = self._parse_transaction(item, account_id, account_name)
                if txn:
                    transactions.append(txn)
            
            # Check if we got all transactions
            total = data.get('totalTransactionsCount', 0)
            offset += len(items)
            
            if offset >= total:
                break
        
        logger.info(f"Got {len(transactions)} pending transactions for account {account_id}")
        return transactions
    
    def _parse_transaction(self, item: dict, account_id: str, 
                           account_name: str) -> Optional[PendingTransaction]:
        """Parse transaction from API response"""
        try:
            amount = float(item.get('amount', 0))
            txn_type = 'spent' if amount < 0 else 'received'
            
            # Parse date
            date_str = item.get('olbTxnDate', '')
            if date_str:
                # Convert ISO format to simple date
                try:
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    date_str = dt.strftime('%Y-%m-%d')
                except:
                    pass
            
            # Get suggested category name
            category = None
            add_info = item.get('addAsQboTxn', {})
            details = add_info.get('details', [])
            if details and 'categoryId' in details[0]:
                category = str(details[0]['categoryId'])  # Just ID for now
            
            return PendingTransaction(
                id=item.get('id', ''),
                olb_txn_id=str(item.get('olbTxnId', '')),
                date=date_str,
                description=item.get('description', ''),
                original_description=item.get('origDescription', ''),
                amount=abs(amount),
                transaction_type=txn_type,
                account_id=account_id,
                account_name=account_name,
                merchant_name=item.get('merchantName'),
                suggested_category=category,
                category_explanation=item.get('categoryExplanation'),
                confidence=item.get('suggestionConfidence')
            )
        except Exception as e:
            logger.warning(f"Error parsing transaction: {e}")
            return None


# =============================================================================
# Main Scraper
# =============================================================================

class QBBankScraper:
    """Main scraper orchestrator"""
    
    def __init__(self, state_file: Path = STATE_FILE):
        self.session = QBSession(state_file)
        self.client = QBHttpClient(self.session)
    
    def check_session(self) -> bool:
        """Check if session is valid"""
        return self.client.check_session()
    
    def scrape_all_pending(self) -> tuple[List[BankAccount], List[PendingTransaction]]:
        """Scrape pending transactions from all accounts"""
        accounts, raw_data = self.client.get_accounts()
        all_transactions = []
        
        for account in accounts:
            # Skip accounts with no pending transactions (if we have that info)
            # Actually, let's check all accounts since pending_count might not be in the response
            
            transactions = self.client.get_pending_transactions(
                account_id=account.qbo_account_id,
                account_name=account.name
            )
            all_transactions.extend(transactions)
        
        logger.info(f"Total: {len(all_transactions)} pending transactions across {len(accounts)} accounts")
        return accounts, all_transactions
    
    def scrape_account(self, account_id: str) -> List[PendingTransaction]:
        """Scrape pending transactions for a specific account"""
        return self.client.get_pending_transactions(account_id)


# =============================================================================
# QuickBase Sync
# =============================================================================

class QuickBaseSync:
    """Syncs scraped transactions to QuickBase"""
    
    # Field mapping - update these to match your QB table
    FIELD_MAP = {
        'id': 6,                    # Text - QB transaction ID
        'olb_txn_id': 7,           # Text - OLB transaction ID
        'date': 8,                  # Date
        'description': 9,           # Text
        'original_description': 10, # Text
        'amount': 11,               # Numeric
        'transaction_type': 12,     # Text - 'spent' or 'received'
        'account_id': 13,           # Text
        'account_name': 14,         # Text
        'merchant_name': 15,        # Text
        'suggested_category': 16,   # Text
        'confidence': 17,           # Text
        'scraped_at': 18,           # DateTime
        'unique_key': 19,           # Text - for deduplication (merge field)
    }
    
    def __init__(self, realm: str = None, token: str = None, table_id: str = None):
        self.realm = realm or QUICKBASE_REALM
        self.token = token or QUICKBASE_TOKEN
        self.table_id = table_id or QUICKBASE_TABLE_ID
        self.base_url = "https://api.quickbase.com/v1"
    
    def _get_headers(self) -> Dict[str, str]:
        return {
            'QB-Realm-Hostname': f'{self.realm}.quickbase.com',
            'Authorization': f'QB-USER-TOKEN {self.token}',
            'Content-Type': 'application/json'
        }
    
    def sync_transactions(self, transactions: List[PendingTransaction]) -> Dict:
        """Sync transactions to QuickBase using upsert"""
        if not all([self.realm, self.token, self.table_id]):
            logger.error("QuickBase not configured")
            return {'success': False, 'error': 'QuickBase not configured'}
        
        if not transactions:
            return {'success': True, 'created': 0, 'updated': 0}
        
        # Build records
        records = []
        for txn in transactions:
            record = {}
            for field_name, field_id in self.FIELD_MAP.items():
                value = getattr(txn, field_name, None)
                if field_name == 'unique_key':
                    value = txn.unique_key
                if value is not None:
                    record[str(field_id)] = {'value': value}
            records.append(record)
        
        # Upsert to QuickBase
        response = requests.post(
            f"{self.base_url}/records",
            headers=self._get_headers(),
            json={
                'to': self.table_id,
                'data': records,
                'mergeFieldId': self.FIELD_MAP['unique_key']
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            metadata = data.get('metadata', {})
            return {
                'success': True,
                'created': len(metadata.get('createdRecordIds', [])),
                'updated': len(metadata.get('updatedRecordIds', [])),
                'unchanged': len(metadata.get('unchangedRecordIds', []))
            }
        else:
            logger.error(f"QuickBase sync failed: {response.text}")
            return {'success': False, 'error': response.text}


# =============================================================================
# Playwright Login (for initial session creation)
# =============================================================================

def create_session_with_playwright():
    """Create a new session using Playwright for manual login"""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
        return False
    
    print("\n" + "="*60)
    print("QUICKBOOKS LOGIN")
    print("="*60)
    print("1. A browser window will open")
    print("2. Log in to QuickBooks")
    print("3. Navigate to the Banking page")
    print("4. Return here and press Enter")
    print("="*60 + "\n")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        
        page.goto('https://qbo.intuit.com')
        
        input(">>> Press Enter after you've logged in and see the Banking page... ")
        
        # Verify we're logged in
        if 'app/' in page.url or 'banking' in page.url:
            # Save session
            context.storage_state(path=str(STATE_FILE))
            print(f"\n✓ Session saved to {STATE_FILE}")
            
            # Verify we can parse it
            try:
                session = QBSession(STATE_FILE)
                print(f"✓ Company ID: {session.company_id}")
                print(f"✓ Session appears valid")
                success = True
            except Exception as e:
                print(f"✗ Error validating session: {e}")
                success = False
        else:
            print(f"✗ Login may have failed. Current URL: {page.url}")
            success = False
        
        context.close()
        browser.close()
        
    return success


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='QuickBooks Bank Transaction Scraper (HTTP API)')
    parser.add_argument('--login', action='store_true', help='Login via Playwright and save session')
    parser.add_argument('--check', action='store_true', help='Check if session is valid')
    parser.add_argument('--scrape', action='store_true', help='Scrape pending transactions')
    parser.add_argument('--sync', action='store_true', help='Scrape and sync to QuickBase')
    parser.add_argument('--output', type=str, help='Output JSON file path')
    parser.add_argument('--account', type=str, help='Specific account ID to scrape')
    
    args = parser.parse_args()
    
    if not any([args.login, args.check, args.scrape, args.sync]):
        parser.print_help()
        return
    
    # Login mode
    if args.login:
        success = create_session_with_playwright()
        sys.exit(0 if success else 1)
    
    # Check session
    if args.check:
        try:
            scraper = QBBankScraper()
            if scraper.check_session():
                print("✓ Session is valid")
                sys.exit(0)
            else:
                print("✗ Session is invalid or expired")
                sys.exit(1)
        except FileNotFoundError:
            print("✗ No session file found. Run with --login first.")
            sys.exit(1)
    
    # Scrape/sync mode
    if args.scrape or args.sync:
        try:
            scraper = QBBankScraper()
        except FileNotFoundError:
            logger.error("No session file found. Run with --login first.")
            sys.exit(1)
        
        # Check session validity
        if not scraper.check_session():
            logger.error("Session invalid or expired. Run with --login to re-authenticate.")
            sys.exit(1)
        
        # Scrape
        if args.account:
            transactions = scraper.scrape_account(args.account)
            accounts = []
        else:
            accounts, transactions = scraper.scrape_all_pending()
        
        # Save output
        output_file = args.output or str(DATA_DIR / 'pending_transactions.json')
        output_data = {
            'scraped_at': datetime.now(timezone.utc).isoformat(),
            'accounts': [asdict(a) for a in accounts],
            'transactions': [asdict(t) for t in transactions]
        }
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        logger.info(f"Saved {len(transactions)} transactions to {output_file}")
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"SCRAPE COMPLETE")
        print(f"{'='*60}")
        print(f"Accounts: {len(accounts)}")
        print(f"Pending transactions: {len(transactions)}")
        
        # Breakdown by account
        by_account = {}
        for t in transactions:
            key = t.account_name or t.account_id
            by_account[key] = by_account.get(key, 0) + 1
        
        for acct, count in sorted(by_account.items()):
            print(f"  {acct}: {count}")
        print(f"{'='*60}\n")
        
        # Sync to QuickBase
        if args.sync:
            sync = QuickBaseSync()
            result = sync.sync_transactions(transactions)
            
            if result.get('success'):
                logger.info(f"QuickBase sync: {result.get('created', 0)} created, "
                           f"{result.get('updated', 0)} updated")
            else:
                logger.error(f"QuickBase sync failed: {result.get('error')}")
                sys.exit(1)


if __name__ == '__main__':
    main()
