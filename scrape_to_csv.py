#!/usr/bin/env python3
"""
Full test: Login, scrape accounts + transactions, export to CSV
With human-like delays to avoid CAPTCHA
"""

import os
import sys
import time
import csv
import random
from playwright.sync_api import sync_playwright
import requests

QB_USERNAME = os.getenv('QB_USERNAME')
QB_PASSWORD = os.getenv('QB_PASSWORD')

QB_API_KEY = 'prdakyresxaDrhFXaSARXaUdj1S8M7h6YK7YGekc'
QB_BASE_URL = 'https://qbo.intuit.com'


def human_delay(min_sec=1, max_sec=3):
    """Random delay to look more human"""
    time.sleep(random.uniform(min_sec, max_sec))


def login():
    """Login and return cookies"""
    print("="*60)
    print("STEP 1: LOGIN")
    print("="*60)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=['--disable-blink-features=AutomationControlled']
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
            viewport={'width': 1280, 'height': 800},
            locale='en-US',
            timezone_id='America/Denver'
        )
        page = context.new_page()
        
        print("Navigating to QB...")
        page.goto('https://qbo.intuit.com', timeout=60000)
        human_delay(3, 5)
        
        if 'qbo.intuit.com/app/' not in page.url:
            # Enter email with human-like typing
            print("Entering email...")
            email_input = page.wait_for_selector(
                '[data-testid="IdentifierFirstInternationalUserIdInput"]',
                timeout=15000
            )
            human_delay(0.5, 1)
            email_input.click()
            human_delay(0.3, 0.7)
            
            # Type slower with variable delay
            page.keyboard.type(QB_USERNAME, delay=random.randint(80, 150))
            human_delay(0.5, 1.5)
            
            print("Clicking Sign in...")
            signin_btn = page.query_selector('[data-testid="IdentifierFirstSubmitButton"]')
            if signin_btn:
                signin_btn.click()
            human_delay(3, 5)
            
            # Check for CAPTCHA and wait if needed
            if 'captcha' in page.content().lower() or 'robot' in page.content().lower():
                print("\n⚠️  CAPTCHA detected - please solve it manually...")
                page.wait_for_selector('input[type="password"]', timeout=120000)  # Wait up to 2 min
            
            # Password
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
            
            print("Waiting for redirect...")
            page.wait_for_url('**/qbo.intuit.com/app/**', timeout=60000)
        
        # Go to banking
        human_delay(2, 4)
        page.goto('https://qbo.intuit.com/app/banking', timeout=30000)
        human_delay(3, 5)
        
        # Get cookies
        cookies = {}
        for c in context.cookies():
            if 'intuit.com' in c.get('domain', ''):
                cookies[c['name']] = c['value']
        
        print(f"✓ Logged in. Company ID: {cookies.get('qbo.currentcompanyid')}")
        
        browser.close()
        return cookies


def scrape(cookies):
    """Scrape accounts and transactions"""
    print("\n" + "="*60)
    print("STEP 2: SCRAPE")
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
        print(f"ERROR: {resp.status_code} - {resp.text[:200]}")
        return [], []
    
    data = resp.json()
    accounts = data.get('accounts', [])
    print(f"Found {len(accounts)} accounts")
    
    # Scrape transactions for each account
    all_transactions = []
    
    for acct in accounts:
        acct_id = str(acct.get('qboAccountId', ''))
        acct_name = acct.get('qboAccountFullName') or acct.get('olbAccountNickname', 'Unknown')
        pending_count = acct.get('numTxnToReview', 0)
        
        print(f"  Scraping: {acct_name} ({pending_count} pending)...")
        
        resp = requests.get(
            f'{QB_BASE_URL}/api/neo/v1/company/{company_id}/olb/ng/getTransactions',
            params={
                'accountId': acct_id,
                'sort': '-txnDate',
                'reviewState': 'PENDING',
                'ignoreMatching': 'false'
            },
            headers={**headers, 'X-Range': 'items=0-199'},
            timeout=30
        )
        
        if resp.status_code != 200:
            print(f"    ERROR: {resp.status_code}")
            continue
        
        items = resp.json().get('items', [])
        print(f"    Got {len(items)} transactions")
        
        for item in items:
            amount = float(item.get('amount', 0))
            all_transactions.append({
                'id': item.get('id', ''),
                'olb_txn_id': str(item.get('olbTxnId', '')),
                'date': item.get('olbTxnDate', '')[:10] if item.get('olbTxnDate') else '',
                'description': item.get('description', ''),
                'amount': abs(amount),
                'type': 'spent' if amount < 0 else 'received',
                'account_id': acct_id,
                'account_name': acct_name,
                'merchant_name': item.get('merchantName', ''),
                'suggested_category': item.get('suggestedCategory', {}).get('name', '') if item.get('suggestedCategory') else '',
            })
    
    return accounts, all_transactions


def export_csv(accounts, transactions):
    """Export to CSV files"""
    print("\n" + "="*60)
    print("STEP 3: EXPORT CSV")
    print("="*60)
    
    accounts_file = 'qb_accounts.csv'
    with open(accounts_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['account_id', 'account_name', 'nickname', 'institution', 'type', 
                        'bank_balance', 'qbo_balance', 'pending_txns', 'last_updated', 'account_number'])
        for a in accounts:
            writer.writerow([
                a.get('qboAccountId', ''),
                a.get('qboAccountFullName', ''),
                a.get('olbAccountNickname', ''),
                a.get('fiName', ''),
                a.get('qboAccountType', '').replace('&amp;', '&'),
                a.get('bankBalance', ''),
                a.get('qboBalance', ''),
                a.get('numTxnToReview', 0),
                a.get('lastUpdateTime', ''),
                a.get('olbAccountNumber', ''),
            ])
    print(f"✓ Wrote {len(accounts)} accounts to {accounts_file}")
    
    txn_file = 'qb_transactions.csv'
    with open(txn_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'olb_txn_id', 'date', 'description', 'amount', 'type', 
                        'account_id', 'account_name', 'merchant_name', 'suggested_category'])
        for t in transactions:
            writer.writerow([
                t['id'], t['olb_txn_id'], t['date'], t['description'], t['amount'],
                t['type'], t['account_id'], t['account_name'], t['merchant_name'],
                t['suggested_category']
            ])
    print(f"✓ Wrote {len(transactions)} transactions to {txn_file}")
    
    return accounts_file, txn_file


if __name__ == '__main__':
    if not QB_USERNAME or not QB_PASSWORD:
        print("Set QB_USERNAME and QB_PASSWORD")
        sys.exit(1)
    
    cookies = login()
    accounts, transactions = scrape(cookies)
    export_csv(accounts, transactions)
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)