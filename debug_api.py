#!/usr/bin/env python3
"""
Dump raw API response to see actual field names
"""

import os
import sys
import time
import json
from playwright.sync_api import sync_playwright
import requests

QB_USERNAME = os.getenv('QB_USERNAME')
QB_PASSWORD = os.getenv('QB_PASSWORD')

QB_API_KEY = 'prdakyresxaDrhFXaSARXaUdj1S8M7h6YK7YGekc'
QB_BASE_URL = 'https://qbo.intuit.com'


def login():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        
        page.goto('https://qbo.intuit.com', timeout=60000)
        time.sleep(5)
        
        if 'qbo.intuit.com/app/' not in page.url:
            email_input = page.wait_for_selector('[data-testid="IdentifierFirstInternationalUserIdInput"]', timeout=10000)
            email_input.click()
            time.sleep(0.5)
            page.keyboard.type(QB_USERNAME, delay=50)
            time.sleep(0.5)
            
            signin_btn = page.query_selector('[data-testid="IdentifierFirstSubmitButton"]')
            if signin_btn:
                signin_btn.click()
            time.sleep(3)
            
            password_input = page.wait_for_selector('input[type="password"]:not([data-testid="SignInHiddenInput"])', timeout=10000)
            password_input.click()
            time.sleep(0.5)
            page.keyboard.type(QB_PASSWORD, delay=50)
            time.sleep(0.5)
            
            signin_btn = page.query_selector('button[type="submit"]')
            if signin_btn:
                signin_btn.click()
            
            page.wait_for_url('**/qbo.intuit.com/app/**', timeout=30000)
        
        page.goto('https://qbo.intuit.com/app/banking', timeout=30000)
        time.sleep(3)
        
        cookies = {}
        for c in context.cookies():
            if 'intuit.com' in c.get('domain', ''):
                cookies[c['name']] = c['value']
        
        browser.close()
        return cookies


cookies = login()
company_id = cookies.get('qbo.currentcompanyid')

headers = {
    'Accept': '*/*',
    'apiKey': QB_API_KEY,
    'Authorization': f'Intuit_APIKey intuit_apikey={QB_API_KEY}, intuit_apikey_version=1.0',
    'authType': 'browser_auth',
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

print("="*60)
print("RAW ACCOUNTS RESPONSE")
print("="*60)
data = resp.json()

# Save full response
with open('raw_accounts.json', 'w') as f:
    json.dump(data, f, indent=2)
print("Saved full response to raw_accounts.json")

# Show first account structure
if data.get('accounts'):
    print("\nFirst account keys:")
    print(json.dumps(data['accounts'][0], indent=2))
else:
    print("\nNo 'accounts' key. Top-level keys:")
    print(list(data.keys()))
    print("\nFull response:")
    print(json.dumps(data, indent=2)[:2000])