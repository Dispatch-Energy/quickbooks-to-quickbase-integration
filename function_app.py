"""
QuickBooks → QuickBase Sync Azure Function

Timer-triggered function that:
1. Logs into QuickBooks via Playwright
2. Scrapes bank accounts and pending transactions
3. Syncs to QuickBase (accounts as parents, transactions as children)
"""

import azure.functions as func
import logging
import os
import time
import random
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from playwright.sync_api import sync_playwright
import requests

app = func.FunctionApp()

# =============================================================================
# Configuration (from environment / Key Vault)
# =============================================================================

QB_USERNAME = os.getenv('QB_USERNAME')
QB_PASSWORD = os.getenv('QB_PASSWORD')
QB_API_KEY = 'prdakyresxaDrhFXaSARXaUdj1S8M7h6YK7YGekc'
QB_BASE_URL = 'https://qbo.intuit.com'

QUICKBASE_REALM = os.getenv('QUICKBASE_REALM', 'dispatchenergy')
QUICKBASE_TOKEN = os.getenv('QUICKBASE_TOKEN')
ACCOUNTS_TABLE_ID = os.getenv('ACCOUNTS_TABLE_ID')
TRANSACTIONS_TABLE_ID = os.getenv('TRANSACTIONS_TABLE_ID')

# Field mappings
ACCOUNT_FIELDS = {
    'quickbooks_id': 6,
    'account_name': 7,
    'nickname': 8,
    'institution': 9,
    'type': 10,
    'balance': 11,
    'qb_balance': 12,
    'pending_txns': 13,
    'last_updated': 14,
    'last_synced': 15,
}

TRANSACTION_FIELDS = {
    'quickbooks_id': 6,
    'internal_id': 7,
    'date': 8,
    'description': 9,
    'amount': 10,
    'type': 11,
    'merchant_name': 12,
    'related_account': 13,
}


# =============================================================================
# QuickBooks Login
# =============================================================================

def human_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))


def auto_login() -> Dict[str, str]:
    """Login to QuickBooks via Playwright, return session cookies."""
    logging.info("Starting Playwright auto-login...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
            ]
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
            viewport={'width': 1280, 'height': 800},
            timezone_id='America/Denver'
        )
        page = context.new_page()
        
        logging.info("Navigating to QuickBooks...")
        page.goto('https://qbo.intuit.com', timeout=60000)
        human_delay(3, 5)
        
        if 'qbo.intuit.com/app/' not in page.url:
            logging.info(f"On login page: {page.url}")
            
            # Check for remembered account tile
            account_tile = page.query_selector(f'text="{QB_USERNAME}"')
            if account_tile:
                logging.info("Found remembered account - clicking...")
                account_tile.click()
                human_delay(2, 3)
            else:
                # Enter email
                logging.info("Entering email...")
                email_input = page.wait_for_selector(
                    '[data-testid="IdentifierFirstInternationalUserIdInput"]',
                    timeout=15000
                )
                human_delay(0.5, 1)
                email_input.click()
                human_delay(0.3, 0.7)
                page.keyboard.type(QB_USERNAME, delay=random.randint(80, 150))
                human_delay(0.5, 1.5)
                
                signin_btn = page.query_selector('[data-testid="IdentifierFirstSubmitButton"]')
                if signin_btn:
                    signin_btn.click()
                human_delay(3, 5)
            
            # Enter password
            logging.info("Entering password...")
            password_input = page.wait_for_selector(
                'input[type="password"]:not([data-testid="SignInHiddenInput"])',
                timeout=15000
            )
            human_delay(0.5, 1)
            password_input.click()
            human_delay(0.3, 0.7)
            page.keyboard.type(QB_PASSWORD, delay=random.randint(80, 150))
            human_delay(0.5, 1.5)
            
            signin_btn = page.query_selector('button[type="submit"]')
            if signin_btn:
                signin_btn.click()
            
            # Wait for redirect
            logging.info("Waiting for redirect...")
            try:
                page.wait_for_url('**/qbo.intuit.com/app/**', timeout=60000)
                logging.info("Login successful!")
            except:
                page_content = page.content().lower()
                if 'captcha' in page_content or 'robot' in page_content:
                    raise Exception("CAPTCHA triggered - manual intervention required")
                if 'verification' in page_content or 'security code' in page_content:
                    raise Exception("2FA/MFA required - cannot automate")
                raise Exception(f"Login failed - stuck on: {page.url}")
        
        # Navigate to banking
        human_delay(2, 3)
        page.goto('https://qbo.intuit.com/app/banking', timeout=30000)
        human_delay(3, 5)
        
        # Extract cookies
        cookies = {}
        for c in context.cookies():
            if 'intuit.com' in c.get('domain', ''):
                cookies[c['name']] = c['value']
        
        company_id = cookies.get('qbo.currentcompanyid')
        logging.info(f"Got session for company: {company_id}")
        
        browser.close()
        return cookies


# =============================================================================
# QuickBooks Scraping
# =============================================================================

def scrape_quickbooks(cookies: Dict[str, str]) -> Tuple[List, List]:
    """Scrape accounts and transactions from QuickBooks."""
    logging.info("Scraping QuickBooks...")
    
    company_id = cookies.get('qbo.currentcompanyid')
    
    headers = {
        'Accept': '*/*',
        'apiKey': QB_API_KEY,
        'Authorization': f'Intuit_APIKey intuit_apikey={QB_API_KEY}, intuit_apikey_version=1.0',
        'authType': 'browser_auth',
        'Content-Type': 'application/json',
        'Cookie': '; '.join(f'{k}={v}' for k, v in cookies.items()),
        'intuit-company-id': company_id,
        'Referer': f'{QB_BASE_URL}/app/banking',
    }
    
    if cookies.get('qbo.csrftoken'):
        headers['Csrftoken'] = cookies['qbo.csrftoken']
    
    # Get accounts
    resp = requests.get(
        f'{QB_BASE_URL}/api/neo/v1/company/{company_id}/olb/ng/getInitialData',
        headers=headers,
        timeout=30
    )
    
    if resp.status_code != 200:
        raise Exception(f"Failed to get accounts: {resp.status_code} - {resp.text[:200]}")
    
    accounts = resp.json().get('accounts', [])
    logging.info(f"Found {len(accounts)} accounts")
    
    # Scrape transactions
    all_transactions = []
    
    for acct in accounts:
        acct_id = str(acct.get('qboAccountId', ''))
        acct_name = acct.get('qboAccountFullName') or acct.get('olbAccountNickname', 'Unknown')
        pending_count = acct.get('numTxnToReview', 0)
        
        logging.info(f"  {acct_name}: {pending_count} pending...")
        
        resp = requests.get(
            f'{QB_BASE_URL}/api/neo/v1/company/{company_id}/olb/ng/getTransactions',
            params={
                'accountId': acct_id,
                'sort': '-txnDate',
                'reviewState': 'PENDING',
                'ignoreMatching': 'false'
            },
            headers={**headers, 'X-Range': 'items=0-499'},
            timeout=30
        )
        
        if resp.status_code != 200:
            logging.warning(f"    Failed: {resp.status_code}")
            continue
        
        items = resp.json().get('items', [])
        
        for item in items:
            amount = float(item.get('amount', 0))
            all_transactions.append({
                'id': item.get('id', ''),
                'olb_txn_id': str(item.get('olbTxnId', '')),
                'date': item.get('olbTxnDate', '')[:10] if item.get('olbTxnDate') else '',
                'description': item.get('description', ''),
                'amount': abs(amount),
                'type': 'Expense' if amount < 0 else 'Income',
                'account_id': acct_id,
                'account_name': acct_name,
                'merchant_name': item.get('merchantName', ''),
            })
    
    logging.info(f"Total: {len(accounts)} accounts, {len(all_transactions)} transactions")
    return accounts, all_transactions


# =============================================================================
# QuickBase Sync
# =============================================================================

def quickbase_request(method: str, endpoint: str, data: dict = None):
    """Make QuickBase API request."""
    headers = {
        'QB-Realm-Hostname': f'{QUICKBASE_REALM}.quickbase.com',
        'Authorization': f'QB-USER-TOKEN {QUICKBASE_TOKEN}',
        'Content-Type': 'application/json',
    }
    
    url = f'https://api.quickbase.com/v1/{endpoint}'
    resp = requests.request(method, url, headers=headers, json=data, timeout=30)
    
    if resp.status_code not in [200, 207]:
        logging.error(f"QuickBase Error: {resp.status_code} - {resp.text[:500]}")
    
    return resp


def sync_accounts(accounts: List) -> Dict[str, int]:
    """Sync accounts to QuickBase, return mapping of qboAccountId → Record ID#."""
    logging.info("Syncing accounts to QuickBase...")
    
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    records = []
    for a in accounts:
        last_updated = a.get('lastUpdateTime', '')
        if last_updated:
            try:
                parsed = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                last_updated = parsed.strftime('%Y-%m-%dT%H:%M:%SZ')
            except:
                last_updated = ''
        
        records.append({
            str(ACCOUNT_FIELDS['quickbooks_id']): {'value': int(a.get('qboAccountId', 0))},
            str(ACCOUNT_FIELDS['account_name']): {'value': a.get('qboAccountFullName', '')},
            str(ACCOUNT_FIELDS['nickname']): {'value': a.get('olbAccountNickname', '')},
            str(ACCOUNT_FIELDS['institution']): {'value': a.get('fiName', '')},
            str(ACCOUNT_FIELDS['type']): {'value': a.get('qboAccountType', '').replace('&amp;', '&')},
            str(ACCOUNT_FIELDS['balance']): {'value': float(a.get('bankBalance', 0) or 0)},
            str(ACCOUNT_FIELDS['qb_balance']): {'value': float(a.get('qboBalance', 0) or 0)},
            str(ACCOUNT_FIELDS['pending_txns']): {'value': str(a.get('numTxnToReview', 0))},
            str(ACCOUNT_FIELDS['last_updated']): {'value': last_updated},
            str(ACCOUNT_FIELDS['last_synced']): {'value': now},
        })
    
    resp = quickbase_request('POST', 'records', {
        'to': ACCOUNTS_TABLE_ID,
        'data': records,
        'mergeFieldId': ACCOUNT_FIELDS['quickbooks_id'],
        'fieldsToReturn': [3, ACCOUNT_FIELDS['quickbooks_id']],
    })
    
    if resp.status_code != 200:
        raise Exception(f"Account sync failed: {resp.text[:500]}")
    
    result = resp.json()
    meta = result.get('metadata', {})
    logging.info(f"  Created: {len(meta.get('createdRecordIds', []))}, Updated: {len(meta.get('updatedRecordIds', []))}")
    
    # Build mapping
    account_map = {}
    for record in result.get('data', []):
        qb_id = record.get(str(ACCOUNT_FIELDS['quickbooks_id']), {}).get('value')
        record_id = record.get('3', {}).get('value')
        if qb_id is not None and record_id:
            account_map[str(int(qb_id))] = record_id
    
    # Query for complete mapping
    resp = quickbase_request('POST', 'records/query', {
        'from': ACCOUNTS_TABLE_ID,
        'select': [3, ACCOUNT_FIELDS['quickbooks_id']],
    })
    
    if resp.status_code == 200:
        for record in resp.json().get('data', []):
            qb_id = record.get(str(ACCOUNT_FIELDS['quickbooks_id']), {}).get('value')
            record_id = record.get('3', {}).get('value')
            if qb_id is not None and record_id:
                account_map[str(int(qb_id))] = record_id
    
    logging.info(f"  Mapped {len(account_map)} accounts")
    return account_map


def sync_transactions(transactions: List, account_map: Dict[str, int]):
    """Sync transactions to QuickBase as children of accounts."""
    logging.info("Syncing transactions to QuickBase...")
    
    records = []
    skipped = 0
    
    for t in transactions:
        parent_record_id = account_map.get(str(t['account_id']))
        
        if not parent_record_id:
            skipped += 1
            continue
        
        internal_id = t['id']
        if internal_id:
            numeric_part = ''.join(c for c in str(internal_id).split(':')[0] if c.isdigit())
            internal_id = int(numeric_part) if numeric_part else 0
        else:
            internal_id = 0
        
        records.append({
            str(TRANSACTION_FIELDS['quickbooks_id']): {'value': t['olb_txn_id']},
            str(TRANSACTION_FIELDS['internal_id']): {'value': internal_id},
            str(TRANSACTION_FIELDS['date']): {'value': t['date']},
            str(TRANSACTION_FIELDS['description']): {'value': t['description']},
            str(TRANSACTION_FIELDS['amount']): {'value': t['amount']},
            str(TRANSACTION_FIELDS['type']): {'value': t['type']},
            str(TRANSACTION_FIELDS['merchant_name']): {'value': t['merchant_name']},
            str(TRANSACTION_FIELDS['related_account']): {'value': parent_record_id},
        })
    
    if skipped:
        logging.warning(f"  Skipped {skipped} transactions (no matching account)")
    
    if not records:
        logging.info("  No transactions to sync")
        return
    
    # Batch upsert
    batch_size = 1000
    total_created = 0
    total_updated = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        logging.info(f"  Batch {i//batch_size + 1}: {len(batch)} transactions...")
        
        resp = quickbase_request('POST', 'records', {
            'to': TRANSACTIONS_TABLE_ID,
            'data': batch,
            'mergeFieldId': TRANSACTION_FIELDS['quickbooks_id'],
        })
        
        if resp.status_code == 200:
            meta = resp.json().get('metadata', {})
            total_created += len(meta.get('createdRecordIds', []))
            total_updated += len(meta.get('updatedRecordIds', []))
    
    logging.info(f"  Total: {total_created} created, {total_updated} updated")


# =============================================================================
# Azure Function Entry Point
# =============================================================================

@app.schedule(
    schedule="0 0 13 * * *",  # 1 PM UTC = 6 AM MST
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def qb_sync_timer(timer: func.TimerRequest) -> None:
    """Timer-triggered QuickBooks to QuickBase sync."""
    
    if timer.past_due:
        logging.warning("Timer is past due!")
    
    logging.info("Starting QuickBooks → QuickBase sync...")
    start_time = time.time()
    
    try:
        # Validate config
        missing = []
        if not QB_USERNAME: missing.append('QB_USERNAME')
        if not QB_PASSWORD: missing.append('QB_PASSWORD')
        if not QUICKBASE_TOKEN: missing.append('QUICKBASE_TOKEN')
        if not ACCOUNTS_TABLE_ID: missing.append('ACCOUNTS_TABLE_ID')
        if not TRANSACTIONS_TABLE_ID: missing.append('TRANSACTIONS_TABLE_ID')
        
        if missing:
            raise Exception(f"Missing config: {', '.join(missing)}")
        
        # Run sync
        cookies = auto_login()
        accounts, transactions = scrape_quickbooks(cookies)
        account_map = sync_accounts(accounts)
        sync_transactions(transactions, account_map)
        
        elapsed = time.time() - start_time
        logging.info(f"Sync complete! ({elapsed:.1f}s)")
        
    except Exception as e:
        logging.error(f"Sync failed: {e}")
        raise


@app.route(route="sync", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def qb_sync_manual(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP-triggered manual sync."""
    
    logging.info("Manual sync triggered")
    
    try:
        # Validate config
        missing = []
        if not QB_USERNAME: missing.append('QB_USERNAME')
        if not QB_PASSWORD: missing.append('QB_PASSWORD')
        if not QUICKBASE_TOKEN: missing.append('QUICKBASE_TOKEN')
        if not ACCOUNTS_TABLE_ID: missing.append('ACCOUNTS_TABLE_ID')
        if not TRANSACTIONS_TABLE_ID: missing.append('TRANSACTIONS_TABLE_ID')
        
        if missing:
            return func.HttpResponse(f"Missing config: {', '.join(missing)}", status_code=500)
        
        # Run sync
        cookies = auto_login()
        accounts, transactions = scrape_quickbooks(cookies)
        account_map = sync_accounts(accounts)
        sync_transactions(transactions, account_map)
        
        return func.HttpResponse(
            f"Sync complete: {len(accounts)} accounts, {len(transactions)} transactions",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Sync failed: {e}")
        return func.HttpResponse(f"Sync failed: {e}", status_code=500)


@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint."""
    return func.HttpResponse("OK", status_code=200)
