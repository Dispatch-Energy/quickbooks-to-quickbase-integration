#!/usr/bin/env python3
"""
QuickBooks → QuickBase Sync (v2)

1. Login to QuickBooks (browser scrape for bank feeds)
2. Scrape accounts + pending transactions
3. Sync to QuickBase:
   - Bank accounts (upsert)
   - Bank balances (daily snapshot - insert new record each day)
   - Bank transactions (upsert as children)
4. Optionally sync GL data via OAuth API

Usage:
    # Bank feeds only (default)
    python qb_sync_v2.py
    
    # Bank feeds + GL sync
    python qb_sync_v2.py --with-gl
    
    # GL sync only (requires OAuth tokens already set up)
    python qb_sync_v2.py --gl-only
"""

import os
import sys
import time
import random
import argparse
from datetime import datetime, timezone, date
from playwright.sync_api import sync_playwright
import requests

# =============================================================================
# Configuration
# =============================================================================

# QuickBooks Browser Scraping
QB_USERNAME = os.getenv('QB_USERNAME')
QB_PASSWORD = os.getenv('QB_PASSWORD')
QB_API_KEY = 'prdakyresxaDrhFXaSARXaUdj1S8M7h6YK7YGekc'
QB_BASE_URL = 'https://qbo.intuit.com'

# QuickBase
QUICKBASE_REALM = os.getenv('QUICKBASE_REALM', 'dispatchenergy')
QUICKBASE_TOKEN = os.getenv('QUICKBASE_TOKEN')

# Bank Feeds Tables
ACCOUNTS_TABLE_ID = os.getenv('ACCOUNTS_TABLE_ID')      # Bank Account table
TRANSACTIONS_TABLE_ID = os.getenv('TRANSACTIONS_TABLE_ID')  # Bank Transaction table
BALANCES_TABLE_ID = os.getenv('BALANCES_TABLE_ID')      # Bank Balance table (NEW)

# Bank Account field mappings
ACCOUNT_FIELDS = {
    'quickbooks_id': 6,      # Numeric - merge key
    'account_name': 7,       # Text
    'nickname': 8,           # Text
    'institution': 9,        # Text
    'type': 10,              # Text
    'balance': 11,           # Numeric (bank balance)
    'qb_balance': 12,        # Numeric (QuickBooks balance)
    'pending_txns': 13,      # Text
    'last_updated': 14,      # Date/Time
    'last_synced': 15,       # Date/Time
}

# Bank Transaction field mappings
TRANSACTION_FIELDS = {
    'quickbooks_id': 6,      # Text - merge key (olb_txn_id)
    'internal_id': 7,        # Numeric (id from QB)
    'date': 8,               # Date
    'description': 9,        # Text
    'amount': 10,            # Numeric
    'type': 11,              # Text
    'merchant_name': 12,     # Text
    'related_account': 13,   # Numeric (reference to Bank Account Record ID#)
}

# Bank Balance field mappings (NEW)
# Based on table definition:
# Field 3: Record ID# (built-in)
# Field 6: Balance (Currency)
# Field 7: Date Added (Date)
# Field 8: Related Bank Account (Numeric reference)
BALANCE_FIELDS = {
    'balance': 6,            # Currency - the bank balance
    'date_added': 7,         # Date - the snapshot date
    'related_account': 8,    # Numeric - reference to Bank Account Record ID#
}


def human_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))


def login():
    """Login to QuickBooks and return cookies"""
    print("=" * 60)
    print("STEP 1: LOGIN TO QUICKBOOKS")
    print("=" * 60)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=['--disable-blink-features=AutomationControlled']
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15',
            viewport={'width': 1280, 'height': 800},
            timezone_id='America/Denver'
        )
        page = context.new_page()
        
        print("Navigating to QB...")
        page.goto('https://qbo.intuit.com', timeout=60000)
        human_delay(3, 5)
        
        if 'qbo.intuit.com/app/' not in page.url:
            print("Entering email...")
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
            
            # Check for CAPTCHA
            if 'captcha' in page.content().lower() or 'robot' in page.content().lower():
                print("\n⚠️  CAPTCHA detected - please solve it manually...")
                page.wait_for_selector('input[type="password"]', timeout=120000)
            
            print("Entering password...")
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
            
            page.wait_for_url('**/qbo.intuit.com/app/**', timeout=60000)
        
        human_delay(2, 4)
        page.goto('https://qbo.intuit.com/app/banking', timeout=30000)
        human_delay(3, 5)
        
        cookies = {}
        for c in context.cookies():
            if 'intuit.com' in c.get('domain', ''):
                cookies[c['name']] = c['value']
        
        print(f"✓ Logged in. Company ID: {cookies.get('qbo.currentcompanyid')}")
        browser.close()
        return cookies


def scrape_quickbooks(cookies):
    """Scrape accounts and transactions from QuickBooks"""
    print("\n" + "=" * 60)
    print("STEP 2: SCRAPE QUICKBOOKS")
    print("=" * 60)
    
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
    print("Fetching accounts...")
    resp = requests.get(
        f'{QB_BASE_URL}/api/neo/v1/company/{company_id}/olb/ng/getInitialData',
        headers=headers,
        timeout=30
    )
    
    if resp.status_code != 200:
        raise Exception(f"Failed to get accounts: {resp.status_code}")
    
    accounts = resp.json().get('accounts', [])
    print(f"Found {len(accounts)} accounts")
    
    # Scrape transactions
    all_transactions = []
    
    for acct in accounts:
        acct_id = str(acct.get('qboAccountId', ''))
        acct_name = acct.get('qboAccountFullName') or acct.get('olbAccountNickname', 'Unknown')
        pending_count = acct.get('numTxnToReview', 0)
        
        print(f"  {acct_name}: {pending_count} pending...")
        
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
            print(f"    ERROR: {resp.status_code}")
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
    
    print(f"\nTotal: {len(accounts)} accounts, {len(all_transactions)} transactions")
    return accounts, all_transactions


def quickbase_request(method, endpoint, data=None):
    """Make QuickBase API request"""
    headers = {
        'QB-Realm-Hostname': f'{QUICKBASE_REALM}.quickbase.com',
        'Authorization': f'QB-USER-TOKEN {QUICKBASE_TOKEN}',
        'Content-Type': 'application/json',
    }
    
    url = f'https://api.quickbase.com/v1/{endpoint}'
    
    resp = requests.request(method, url, headers=headers, json=data, timeout=30)
    
    if resp.status_code not in [200, 207]:
        print(f"QuickBase Error: {resp.status_code} - {resp.text[:500]}")
    
    return resp


def sync_accounts(accounts):
    """Sync accounts to QuickBase, return mapping of qboAccountId → Record ID#"""
    print("\n" + "=" * 60)
    print("STEP 3: SYNC ACCOUNTS TO QUICKBASE")
    print("=" * 60)
    
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    records = []
    for a in accounts:
        # Format last_updated if present
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
    
    print(f"Upserting {len(records)} accounts...")
    
    resp = quickbase_request('POST', 'records', {
        'to': ACCOUNTS_TABLE_ID,
        'data': records,
        'mergeFieldId': ACCOUNT_FIELDS['quickbooks_id'],
        'fieldsToReturn': [3, ACCOUNT_FIELDS['quickbooks_id']],
    })
    
    if resp.status_code != 200:
        raise Exception(f"Account sync failed: {resp.text}")
    
    result = resp.json()
    meta = result.get('metadata', {})
    print(f"  Created: {len(meta.get('createdRecordIds', []))}")
    print(f"  Updated: {len(meta.get('updatedRecordIds', []))}")
    
    # Build mapping: qboAccountId → Record ID#
    account_map = {}
    for record in result.get('data', []):
        qb_id = record.get(str(ACCOUNT_FIELDS['quickbooks_id']), {}).get('value')
        record_id = record.get('3', {}).get('value')
        if qb_id is not None and record_id:
            account_map[str(int(qb_id))] = record_id
    
    # Query for all accounts to ensure complete mapping
    print("  Fetching account mapping...")
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
    
    print(f"  Mapped {len(account_map)} accounts")
    return account_map


def sync_bank_balances(accounts, account_map):
    """
    Sync daily bank balance snapshots to QuickBase.
    
    Creates one record per account per day. Uses a composite key approach to 
    prevent duplicates if run multiple times on the same day.
    """
    print("\n" + "=" * 60)
    print("STEP 4: SYNC BANK BALANCES (DAILY SNAPSHOT)")
    print("=" * 60)
    
    if not BALANCES_TABLE_ID:
        print("  ⚠️  BALANCES_TABLE_ID not set, skipping bank balances sync")
        return
    
    today = date.today().isoformat()  # YYYY-MM-DD format
    
    records = []
    skipped = 0
    
    for acct in accounts:
        acct_id = str(acct.get('qboAccountId', ''))
        acct_name = acct.get('qboAccountFullName') or acct.get('olbAccountNickname', 'Unknown')
        
        # Get parent account Record ID#
        parent_record_id = account_map.get(acct_id)
        
        if not parent_record_id:
            skipped += 1
            continue
        
        # Get balance (prefer bank balance, fall back to QB balance)
        balance = float(acct.get('bankBalance', 0) or 0)
        if balance == 0:
            balance = float(acct.get('qboBalance', 0) or 0)
        
        records.append({
            str(BALANCE_FIELDS['balance']): {'value': balance},
            str(BALANCE_FIELDS['date_added']): {'value': today},
            str(BALANCE_FIELDS['related_account']): {'value': parent_record_id},
        })
        
        print(f"  {acct_name}: ${balance:,.2f}")
    
    if skipped:
        print(f"  Skipped {skipped} accounts (no matching parent record)")
    
    if not records:
        print("  No balance records to sync")
        return
    
    # Check if we already have balances for today to avoid duplicates
    # Query for existing records with today's date
    print(f"\n  Checking for existing balances on {today}...")
    
    existing_check = quickbase_request('POST', 'records/query', {
        'from': BALANCES_TABLE_ID,
        'select': [3, BALANCE_FIELDS['date_added'], BALANCE_FIELDS['related_account']],
        'where': f"{{{BALANCE_FIELDS['date_added']}.EX.'{today}'}}"
    })
    
    existing_accounts = set()
    if existing_check.status_code == 200:
        for rec in existing_check.json().get('data', []):
            acct_ref = rec.get(str(BALANCE_FIELDS['related_account']), {}).get('value')
            if acct_ref:
                existing_accounts.add(acct_ref)
    
    if existing_accounts:
        print(f"  Found {len(existing_accounts)} existing balance records for today")
        # Filter out accounts that already have a balance record for today
        original_count = len(records)
        records = [
            r for r in records 
            if r[str(BALANCE_FIELDS['related_account'])]['value'] not in existing_accounts
        ]
        print(f"  Filtered to {len(records)} new balance records")
    
    if not records:
        print("  All balances already synced for today")
        return
    
    # Insert balance records (not upsert - we want historical snapshots)
    print(f"  Inserting {len(records)} balance snapshot records...")
    
    resp = quickbase_request('POST', 'records', {
        'to': BALANCES_TABLE_ID,
        'data': records,
    })
    
    if resp.status_code == 200:
        meta = resp.json().get('metadata', {})
        print(f"  ✓ Created: {len(meta.get('createdRecordIds', []))}")
    else:
        print(f"  ✗ Balance sync failed: {resp.status_code}")


def sync_transactions(transactions, account_map):
    """Sync transactions to QuickBase as children of accounts"""
    print("\n" + "=" * 60)
    print("STEP 5: SYNC TRANSACTIONS TO QUICKBASE")
    print("=" * 60)
    
    records = []
    skipped = 0
    
    for t in transactions:
        parent_record_id = account_map.get(str(t['account_id']))
        
        if not parent_record_id:
            skipped += 1
            continue
        
        # Extract numeric part of ID if it contains non-numeric chars
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
        print(f"  Skipped {skipped} transactions (no matching account)")
    
    if not records:
        print("  No transactions to sync")
        return
    
    # Batch in chunks of 1000
    batch_size = 1000
    total_created = 0
    total_updated = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        print(f"  Upserting batch {i // batch_size + 1}: {len(batch)} transactions...")
        
        resp = quickbase_request('POST', 'records', {
            'to': TRANSACTIONS_TABLE_ID,
            'data': batch,
            'mergeFieldId': TRANSACTION_FIELDS['quickbooks_id'],
        })
        
        if resp.status_code == 200:
            meta = resp.json().get('metadata', {})
            total_created += len(meta.get('createdRecordIds', []))
            total_updated += len(meta.get('updatedRecordIds', []))
        else:
            print(f"    Batch error: {resp.status_code}")
    
    print(f"\n  Total created: {total_created}")
    print(f"  Total updated: {total_updated}")


# =============================================================================
# GL Sync Integration (OAuth-based)
# =============================================================================

def run_gl_sync():
    """
    Run GL sync using the OAuth-based qb_to_quickbase_sync module.
    
    This requires:
    - OAuth tokens already set up (run qb_to_quickbase_sync.py --add-company first)
    - Environment variables: QB_CLIENT_ID, QB_CLIENT_SECRET, QUICKBASE_APP_ID
    """
    print("\n" + "=" * 60)
    print("GL SYNC (OAuth API)")
    print("=" * 60)
    
    try:
        # Import the OAuth sync module
        from qb_to_quickbase_sync import (
            load_config, TokenStore, QBOAuth, QuickBaseClient, SyncEngine
        )
    except ImportError:
        print("  ⚠️  qb_to_quickbase_sync.py not found in path")
        print("  Make sure it's in the same directory or PYTHONPATH")
        return False
    
    config = load_config()
    if not config:
        print("  ⚠️  GL sync config not found. Set environment variables:")
        print("      QB_CLIENT_ID, QB_CLIENT_SECRET, QUICKBASE_REALM,")
        print("      QUICKBASE_TOKEN, QUICKBASE_APP_ID")
        return False
    
    token_store = TokenStore()
    tokens = token_store.get_all()
    
    if not tokens:
        print("  ⚠️  No OAuth tokens found.")
        print("  Run: python qb_to_quickbase_sync.py --add-company")
        return False
    
    print(f"  Found {len(tokens)} connected companies")
    
    oauth = QBOAuth(config.client_id, config.client_secret, token_store)
    qb_client = QuickBaseClient(
        realm=config.quickbase_realm,
        token=config.quickbase_token,
        app_id=config.quickbase_app_id
    )
    
    engine = SyncEngine(oauth, qb_client)
    engine.sync_all()
    
    return True


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='QuickBooks → QuickBase Sync',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables Required:
  QB_USERNAME           QuickBooks login email
  QB_PASSWORD           QuickBooks login password
  QUICKBASE_TOKEN       QuickBase user token
  QUICKBASE_REALM       QuickBase realm (default: dispatchenergy)
  ACCOUNTS_TABLE_ID     Bank Account table ID
  TRANSACTIONS_TABLE_ID Bank Transaction table ID
  BALANCES_TABLE_ID     Bank Balance table ID (for daily snapshots)

For GL Sync (--with-gl or --gl-only):
  QB_CLIENT_ID          QuickBooks OAuth Client ID
  QB_CLIENT_SECRET      QuickBooks OAuth Client Secret
  QUICKBASE_APP_ID      QuickBase App ID

Examples:
  # Bank feeds only (accounts, transactions, balances)
  python qb_sync_v2.py

  # Bank feeds + GL sync
  python qb_sync_v2.py --with-gl

  # GL sync only
  python qb_sync_v2.py --gl-only
        """
    )
    parser.add_argument('--with-gl', action='store_true', 
                        help='Also run GL sync after bank feeds')
    parser.add_argument('--gl-only', action='store_true',
                        help='Run GL sync only (no bank feeds)')
    parser.add_argument('--skip-balances', action='store_true',
                        help='Skip bank balance snapshot sync')
    
    args = parser.parse_args()
    
    # GL only mode
    if args.gl_only:
        run_gl_sync()
        print("\n" + "=" * 60)
        print("✓ GL SYNC COMPLETE")
        print("=" * 60)
        return
    
    # Validate bank feeds config
    missing = []
    if not QB_USERNAME:
        missing.append('QB_USERNAME')
    if not QB_PASSWORD:
        missing.append('QB_PASSWORD')
    if not QUICKBASE_TOKEN:
        missing.append('QUICKBASE_TOKEN')
    if not ACCOUNTS_TABLE_ID:
        missing.append('ACCOUNTS_TABLE_ID')
    if not TRANSACTIONS_TABLE_ID:
        missing.append('TRANSACTIONS_TABLE_ID')
    
    if missing:
        print("Missing environment variables:")
        for m in missing:
            print(f"  - {m}")
        print("\nExample:")
        print('  export QB_USERNAME="it@dispatchenergy.com"')
        print('  export QB_PASSWORD="your-password"')
        print('  export QUICKBASE_TOKEN="your-token"')
        print('  export ACCOUNTS_TABLE_ID="bxxxxxxxx"')
        print('  export TRANSACTIONS_TABLE_ID="bxxxxxxxx"')
        print('  export BALANCES_TABLE_ID="bxxxxxxxx"')
        sys.exit(1)
    
    # Run bank feeds sync
    cookies = login()
    accounts, transactions = scrape_quickbooks(cookies)
    account_map = sync_accounts(accounts)
    
    # Sync bank balances (daily snapshot)
    if not args.skip_balances:
        sync_bank_balances(accounts, account_map)
    
    # Sync transactions
    sync_transactions(transactions, account_map)
    
    print("\n" + "=" * 60)
    print("✓ BANK FEEDS SYNC COMPLETE")
    print("=" * 60)
    
    # Optionally run GL sync
    if args.with_gl:
        print("\n")
        run_gl_sync()
        print("\n" + "=" * 60)
        print("✓ GL SYNC COMPLETE")
        print("=" * 60)


if __name__ == '__main__':
    main()