#!/usr/bin/env python3
"""
QuickBooks Online Bank Transaction Scraper

Scrapes pending/uncategorized bank transactions from QuickBooks Online
and syncs them to QuickBase for cash flow reporting.

Requirements:
    pip install playwright python-dotenv requests
    playwright install chromium

Usage:
    python qb_bank_scraper.py --login      # First time - manual login, saves session
    python qb_bank_scraper.py --scrape     # Scrape transactions (uses saved session)
    python qb_bank_scraper.py --sync       # Scrape and sync to QuickBase
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict

import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext

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
DATA_DIR = Path.home() / '.qb_bank_scraper'
SESSION_DIR = DATA_DIR / 'session'
TRANSACTIONS_FILE = DATA_DIR / 'pending_transactions.json'

# QuickBase config
QUICKBASE_REALM = os.getenv('QUICKBASE_REALM')
QUICKBASE_TOKEN = os.getenv('QUICKBASE_TOKEN')
QUICKBASE_APP_ID = os.getenv('QUICKBASE_APP_ID', 'bvntqcqzm')

# QuickBooks URLs
QB_LOGIN_URL = 'https://qbo.intuit.com'
QB_BANKING_URL_TEMPLATE = 'https://qbo.intuit.com/app/banking?accountId={account_id}'


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class BankTransaction:
    """Represents a pending bank transaction"""
    date: str
    description: str
    amount: float
    transaction_type: str  # 'spent' or 'received'
    account_id: str
    account_name: str
    company_id: str
    company_name: str
    suggested_category: Optional[str] = None
    from_to: Optional[str] = None
    scraped_at: str = None
    
    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.now(timezone.utc).isoformat()
    
    @property
    def unique_key(self) -> str:
        """Generate unique key for deduplication"""
        return f"{self.account_id}_{self.date}_{self.amount}_{self.description[:50]}"


# =============================================================================
# QuickBooks Scraper
# =============================================================================

class QBBankScraper:
    """Scrapes pending bank transactions from QuickBooks Online"""
    
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None
        
        # Ensure data directory exists
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        SESSION_DIR.mkdir(parents=True, exist_ok=True)
    
    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        
        # Load existing session if available
        if (SESSION_DIR / 'state.json').exists():
            self.context = self.browser.new_context(storage_state=str(SESSION_DIR / 'state.json'))
            logger.info("Loaded existing session")
        else:
            self.context = self.browser.new_context()
            logger.info("Created new session")
        
        self.page = self.context.new_page()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
    
    def save_session(self):
        """Save browser session state for reuse"""
        self.context.storage_state(path=str(SESSION_DIR / 'state.json'))
        logger.info(f"Session saved to {SESSION_DIR / 'state.json'}")
    
    def login(self):
        """
        Interactive login - opens browser for manual login.
        Saves session after successful login.
        """
        logger.info("Opening QuickBooks for manual login...")
        logger.info("Please log in manually. Press Enter in the terminal when done.")
        
        self.page.goto(QB_LOGIN_URL)
        
        # Wait for user to complete login
        input("\n>>> Press Enter after you've logged in and see the QB dashboard... ")
        
        # Verify we're logged in by checking for common dashboard elements
        try:
            self.page.wait_for_selector('[data-testid="navbar"]', timeout=5000)
            logger.info("Login successful!")
            self.save_session()
            return True
        except:
            # Try alternative check
            if 'app/' in self.page.url or 'homepage' in self.page.url:
                logger.info("Login successful!")
                self.save_session()
                return True
            else:
                logger.error("Login verification failed. Please try again.")
                return False
    
    def is_logged_in(self) -> bool:
        """Check if we're logged into QuickBooks"""
        try:
            self.page.goto(QB_LOGIN_URL, timeout=30000)
            self.page.wait_for_load_state('networkidle', timeout=15000)
            
            # Check if we're on the dashboard or got redirected to login
            current_url = self.page.url
            if 'accounts.intuit.com' in current_url or 'signin' in current_url.lower():
                logger.info("Session expired - need to login")
                return False
            
            logger.info(f"Logged in - current URL: {current_url}")
            return True
        except Exception as e:
            logger.error(f"Error checking login status: {e}")
            return False
    
    def get_connected_accounts(self) -> List[Dict]:
        """Get list of connected bank accounts"""
        accounts = []
        
        # Navigate to banking page
        self.page.goto('https://qbo.intuit.com/app/banking')
        self.page.wait_for_load_state('networkidle')
        
        # Wait for account tiles to load
        try:
            self.page.wait_for_selector('[data-testid="account-tile"]', timeout=10000)
        except:
            # Try alternative selector
            self.page.wait_for_selector('.bank-account-tile, .account-tile', timeout=10000)
        
        # Extract account information
        account_tiles = self.page.query_selector_all('[data-testid="account-tile"], .bank-account-tile, .account-tile')
        
        for tile in account_tiles:
            try:
                # Extract account name and ID
                name_el = tile.query_selector('.account-name, [data-testid="account-name"]')
                name = name_el.inner_text() if name_el else "Unknown Account"
                
                # Get account ID from the tile's data attributes or link
                link = tile.query_selector('a')
                href = link.get_attribute('href') if link else ''
                account_id = ''
                if 'accountId=' in href:
                    account_id = href.split('accountId=')[1].split('&')[0]
                
                # Get pending count
                pending_el = tile.query_selector('.pending-count, [data-testid="pending-count"]')
                pending_count = 0
                if pending_el:
                    try:
                        pending_text = pending_el.inner_text()
                        pending_count = int(''.join(filter(str.isdigit, pending_text)))
                    except:
                        pass
                
                accounts.append({
                    'id': account_id,
                    'name': name,
                    'pending_count': pending_count
                })
                
            except Exception as e:
                logger.warning(f"Error parsing account tile: {e}")
                continue
        
        logger.info(f"Found {len(accounts)} connected accounts")
        return accounts
    
    def scrape_pending_transactions(self, account_id: str = None, company_id: str = None, 
                                    company_name: str = None) -> List[BankTransaction]:
        """
        Scrape pending/for-review transactions from the Banking tab.
        If account_id is provided, scrapes that specific account.
        Otherwise, scrapes all accounts.
        """
        all_transactions = []
        
        # Navigate to banking page
        if account_id:
            url = f'https://qbo.intuit.com/app/banking?accountId={account_id}'
        else:
            url = 'https://qbo.intuit.com/app/banking'
        
        logger.info(f"Navigating to {url}")
        self.page.goto(url)
        self.page.wait_for_load_state('networkidle')
        
        # Click on "For Review" tab if not already selected
        try:
            for_review_tab = self.page.query_selector('[data-testid="for-review-tab"], .for-review-tab, button:has-text("For Review"), [role="tab"]:has-text("Review")')
            if for_review_tab:
                for_review_tab.click()
                self.page.wait_for_load_state('networkidle')
        except Exception as e:
            logger.warning(f"Could not click For Review tab: {e}")
        
        # Wait for transactions to load
        try:
            self.page.wait_for_selector('table tbody tr, [data-testid="transaction-row"], .transaction-row', timeout=10000)
        except:
            logger.info("No transactions found or table not loaded")
            return all_transactions
        
        # Get current account info from page
        current_account_name = "Unknown"
        try:
            account_header = self.page.query_selector('.account-header, [data-testid="account-name"]')
            if account_header:
                current_account_name = account_header.inner_text()
        except:
            pass
        
        # Scrape transactions
        page_num = 1
        while True:
            logger.info(f"Scraping page {page_num}...")
            
            # Get all transaction rows
            rows = self.page.query_selector_all('table tbody tr, [data-testid="transaction-row"], .transaction-row')
            
            if not rows:
                break
            
            for row in rows:
                try:
                    txn = self._parse_transaction_row(row, account_id or '', current_account_name,
                                                      company_id or '', company_name or '')
                    if txn:
                        all_transactions.append(txn)
                except Exception as e:
                    logger.warning(f"Error parsing transaction row: {e}")
                    continue
            
            # Check for next page
            next_button = self.page.query_selector('[data-testid="next-page"], .next-page, button:has-text("Next")')
            if next_button and next_button.is_enabled():
                next_button.click()
                self.page.wait_for_load_state('networkidle')
                page_num += 1
            else:
                break
        
        logger.info(f"Scraped {len(all_transactions)} pending transactions")
        return all_transactions
    
    def _parse_transaction_row(self, row, account_id: str, account_name: str,
                               company_id: str, company_name: str) -> Optional[BankTransaction]:
        """Parse a single transaction row"""
        
        # Try to get date
        date_el = row.query_selector('[data-testid="date"], .date-cell, td:nth-child(1)')
        date_str = date_el.inner_text().strip() if date_el else ''
        
        # Try to get description
        desc_el = row.query_selector('[data-testid="description"], .description-cell, td:nth-child(2)')
        description = desc_el.inner_text().strip() if desc_el else ''
        
        # Try to get amount - check both spent and received columns
        spent_el = row.query_selector('[data-testid="spent"], .spent-cell, td:nth-child(3)')
        received_el = row.query_selector('[data-testid="received"], .received-cell, td:nth-child(4)')
        
        amount = 0.0
        txn_type = 'spent'
        
        if spent_el:
            spent_text = spent_el.inner_text().strip()
            if spent_text and spent_text != '-':
                amount = self._parse_amount(spent_text)
                txn_type = 'spent'
        
        if received_el and amount == 0:
            received_text = received_el.inner_text().strip()
            if received_text and received_text != '-':
                amount = self._parse_amount(received_text)
                txn_type = 'received'
        
        # Try to get suggested category
        category_el = row.query_selector('[data-testid="category"], .category-cell, .suggested-category')
        category = category_el.inner_text().strip() if category_el else None
        
        # Try to get From/To
        from_to_el = row.query_selector('[data-testid="from-to"], .from-to-cell')
        from_to = from_to_el.inner_text().strip() if from_to_el else None
        
        if not date_str or amount == 0:
            return None
        
        return BankTransaction(
            date=date_str,
            description=description,
            amount=amount,
            transaction_type=txn_type,
            account_id=account_id,
            account_name=account_name,
            company_id=company_id,
            company_name=company_name,
            suggested_category=category,
            from_to=from_to
        )
    
    def _parse_amount(self, amount_str: str) -> float:
        """Parse amount string to float"""
        # Remove currency symbols, commas, spaces
        cleaned = amount_str.replace('$', '').replace(',', '').replace(' ', '').strip()
        try:
            return abs(float(cleaned))
        except ValueError:
            return 0.0


# =============================================================================
# QuickBase Sync
# =============================================================================

class QuickBaseSync:
    """Syncs scraped transactions to QuickBase"""
    
    # Table config - you'll need to create this table in QuickBase
    TABLE_ID = None  # Set this after creating the table
    
    FIELD_MAP = {
        'date': 6,
        'description': 7,
        'amount': 8,
        'transaction_type': 9,
        'account_id': 10,
        'account_name': 11,
        'company_id': 12,
        'company_name': 13,
        'suggested_category': 14,
        'from_to': 15,
        'scraped_at': 16,
        'unique_key': 17,  # For deduplication
    }
    
    def __init__(self, realm: str, token: str, table_id: str = None):
        self.realm = realm
        self.token = token
        self.table_id = table_id or self.TABLE_ID
        self.base_url = "https://api.quickbase.com/v1"
    
    def _get_headers(self) -> Dict[str, str]:
        return {
            'QB-Realm-Hostname': f'{self.realm}.quickbase.com',
            'Authorization': f'QB-USER-TOKEN {self.token}',
            'Content-Type': 'application/json'
        }
    
    def sync_transactions(self, transactions: List[BankTransaction]) -> Dict:
        """Sync transactions to QuickBase"""
        if not self.table_id:
            logger.error("QuickBase table ID not configured")
            return {'created': 0, 'updated': 0, 'errors': ['Table ID not set']}
        
        if not transactions:
            return {'created': 0, 'updated': 0, 'errors': []}
        
        # Build records
        records = []
        for txn in transactions:
            record = {
                str(self.FIELD_MAP['date']): {'value': txn.date},
                str(self.FIELD_MAP['description']): {'value': txn.description},
                str(self.FIELD_MAP['amount']): {'value': txn.amount},
                str(self.FIELD_MAP['transaction_type']): {'value': txn.transaction_type},
                str(self.FIELD_MAP['account_id']): {'value': txn.account_id},
                str(self.FIELD_MAP['account_name']): {'value': txn.account_name},
                str(self.FIELD_MAP['company_id']): {'value': txn.company_id},
                str(self.FIELD_MAP['company_name']): {'value': txn.company_name},
                str(self.FIELD_MAP['suggested_category']): {'value': txn.suggested_category or ''},
                str(self.FIELD_MAP['from_to']): {'value': txn.from_to or ''},
                str(self.FIELD_MAP['scraped_at']): {'value': txn.scraped_at},
                str(self.FIELD_MAP['unique_key']): {'value': txn.unique_key},
            }
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
                'created': len(metadata.get('createdRecordIds', [])),
                'updated': len(metadata.get('updatedRecordIds', [])),
                'errors': []
            }
        else:
            logger.error(f"QuickBase sync failed: {response.text}")
            return {'created': 0, 'updated': 0, 'errors': [response.text]}


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='QuickBooks Bank Transaction Scraper')
    parser.add_argument('--login', action='store_true', help='Interactive login to save session')
    parser.add_argument('--scrape', action='store_true', help='Scrape pending transactions')
    parser.add_argument('--sync', action='store_true', help='Scrape and sync to QuickBase')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--company-id', type=str, help='QuickBooks company/realm ID')
    parser.add_argument('--company-name', type=str, help='Company name for records')
    parser.add_argument('--quickbase-table', type=str, help='QuickBase table ID for sync')
    parser.add_argument('--output', type=str, help='Output JSON file path')
    
    args = parser.parse_args()
    
    if not any([args.login, args.scrape, args.sync]):
        parser.print_help()
        return
    
    # Login mode - always show browser
    if args.login:
        with QBBankScraper(headless=False) as scraper:
            if scraper.login():
                print("\n✓ Session saved! You can now run --scrape or --sync")
            else:
                print("\n✗ Login failed")
                sys.exit(1)
        return
    
    # Scrape mode
    if args.scrape or args.sync:
        with QBBankScraper(headless=args.headless) as scraper:
            # Check if logged in
            if not scraper.is_logged_in():
                logger.error("Not logged in. Run with --login first.")
                sys.exit(1)
            
            # Scrape transactions
            transactions = scraper.scrape_pending_transactions(
                company_id=args.company_id,
                company_name=args.company_name
            )
            
            # Save to file
            output_file = args.output or str(TRANSACTIONS_FILE)
            with open(output_file, 'w') as f:
                json.dump([asdict(t) for t in transactions], f, indent=2)
            logger.info(f"Saved {len(transactions)} transactions to {output_file}")
            
            # Sync to QuickBase if requested
            if args.sync:
                if not QUICKBASE_REALM or not QUICKBASE_TOKEN:
                    logger.error("QuickBase credentials not configured in .env")
                    sys.exit(1)
                
                table_id = args.quickbase_table
                if not table_id:
                    logger.error("QuickBase table ID required for sync (--quickbase-table)")
                    sys.exit(1)
                
                sync = QuickBaseSync(QUICKBASE_REALM, QUICKBASE_TOKEN, table_id)
                result = sync.sync_transactions(transactions)
                
                logger.info(f"Sync complete: {result['created']} created, {result['updated']} updated")
                if result['errors']:
                    for err in result['errors']:
                        logger.error(f"  Error: {err}")


if __name__ == '__main__':
    main()
