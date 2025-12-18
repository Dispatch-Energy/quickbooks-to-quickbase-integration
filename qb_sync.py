#!/usr/bin/env python3
"""
QuickBooks → QuickBase Sync

1. Login to QuickBooks
2. Scrape accounts + transactions
3. Sync to QuickBase (accounts first, then transactions as children)
"""

import os
import sys
import time
import random
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright
import requests

# QuickBooks
QB_USERNAME = os.getenv('QB_USERNAME')
QB_PASSWORD = os.getenv('QB_PASSWORD')
QB_API_KEY = 'prdakyresxaDrhFXaSARXaUdj1S8M7h6YK7YGekc'
QB_BASE_URL = 'https://qbo.intuit.com'

# QuickBase
QUICKBASE_REALM = os.getenv('QUICKBASE_REALM', 'dispatchenergy')
QUICKBASE_TOKEN = os.getenv('QUICKBASE_TOKEN')
ACCOUNTS_TABLE_ID = os.getenv('ACCOUNTS_TABLE_ID')  # Bank Account table
TRANSACTIONS_TABLE_ID = os.getenv('TRANSACTIONS_TABLE_ID')  # Bank Transaction table

# Field mappings based on your schema
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


def human_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))


def login():
    """Login to QuickBooks and return cookies"""
    print("="*60)
    print("STEP 1: LOGIN TO QUICKBOOKS")
    print("="*60)
    
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
    print("\n" + "="*60)
    print("STEP 2: SCRAPE QUICKBOOKS")
    print("="*60)
    
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
    print("\n" + "="*60)
    print("STEP 3: SYNC ACCOUNTS TO QUICKBASE")
    print("="*60)
    
    # QuickBase expects ISO format: "2023-12-17T15:30:00Z"
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    records = []
    for a in accounts:
        # Format last_updated if present
        last_updated = a.get('lastUpdateTime', '')
        if last_updated:
            # Convert "2025-12-16T14:29:30-08:00" to "2025-12-16T22:29:30Z" (UTC)
            try:
                from datetime import datetime as dt
                parsed = dt.fromisoformat(last_updated.replace('Z', '+00:00'))
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
        'fieldsToReturn': [3, ACCOUNT_FIELDS['quickbooks_id']],  # Record ID# and QB ID
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
            account_map[str(int(qb_id))] = record_id  # Convert to int then str to remove .0
    
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


def sync_transactions(transactions, account_map):
    """Sync transactions to QuickBase as children of accounts"""
    print("\n" + "="*60)
    print("STEP 4: SYNC TRANSACTIONS TO QUICKBASE")
    print("="*60)
    
    records = []
    skipped = 0
    
    for t in transactions:
        # Get parent account Record ID#
        parent_record_id = account_map.get(str(t['account_id']))
        
        if not parent_record_id:
            skipped += 1
            continue
        
        # Extract numeric part of ID if it contains non-numeric chars
        internal_id = t['id']
        if internal_id:
            # Handle IDs like '2867:ofx' - extract numeric part
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
        batch = records[i:i+batch_size]
        print(f"  Upserting batch {i//batch_size + 1}: {len(batch)} transactions...")
        
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


def main():
    # Validate config
    missing = []
    if not QB_USERNAME: missing.append('QB_USERNAME')
    if not QB_PASSWORD: missing.append('QB_PASSWORD')
    if not QUICKBASE_TOKEN: missing.append('QUICKBASE_TOKEN')
    if not ACCOUNTS_TABLE_ID: missing.append('ACCOUNTS_TABLE_ID')
    if not TRANSACTIONS_TABLE_ID: missing.append('TRANSACTIONS_TABLE_ID')
    
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
        sys.exit(1)
    
    # Run sync
    cookies = login()
    accounts, transactions = scrape_quickbooks(cookies)
    account_map = sync_accounts(accounts)
    sync_transactions(transactions, account_map)
    
    print("\n" + "="*60)
    print("✓ SYNC COMPLETE")
    print("="*60)


if __name__ == '__main__':
    main()