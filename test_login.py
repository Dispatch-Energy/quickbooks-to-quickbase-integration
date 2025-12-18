#!/usr/bin/env python3
"""
Test QB login - uses keyboard.type() for input
"""

import os
import sys
import time
from playwright.sync_api import sync_playwright

QB_USERNAME = os.getenv('QB_USERNAME')
QB_PASSWORD = os.getenv('QB_PASSWORD')

def test_login(headless=True):
    print(f"Testing login for: {QB_USERNAME}")
    print(f"Headless: {headless}")
    print("-" * 50)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=['--disable-blink-features=AutomationControlled']
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15'
        )
        page = context.new_page()
        
        print("[1] Navigating to qbo.intuit.com...")
        page.goto('https://qbo.intuit.com', timeout=60000)
        time.sleep(5)
        
        print(f"    URL: {page.url}")
        
        # Check if ACTUALLY logged in (must be on qbo.intuit.com/app/, not accounts.intuit.com)
        if 'qbo.intuit.com/app/' in page.url:
            print("[✓] Already logged in!")
        else:
            # Check for remembered account
            account_tile = page.query_selector(f'text="{QB_USERNAME}"')
            if account_tile:
                print("[2] Found remembered account - clicking...")
                account_tile.click()
                time.sleep(2)
            else:
                print("[2] Entering email...")
                email_input = page.wait_for_selector(
                    '[data-testid="IdentifierFirstInternationalUserIdInput"]',
                    timeout=10000
                )
                email_input.click()
                time.sleep(0.5)
                page.keyboard.type(QB_USERNAME, delay=50)
                time.sleep(0.5)
                
                print("    Clicking Sign in...")
                signin_btn = page.query_selector('[data-testid="IdentifierFirstSubmitButton"]')
                if signin_btn:
                    signin_btn.click()
                time.sleep(3)
            
            # Password
            print("[3] Entering password...")
            try:
                password_input = page.wait_for_selector(
                    'input[type="password"]:not([data-testid="SignInHiddenInput"])',
                    timeout=10000
                )
                password_input.click()
                time.sleep(0.5)
                page.keyboard.type(QB_PASSWORD, delay=50)
                time.sleep(0.5)
                
                signin_btn = page.query_selector('button[type="submit"]')
                if signin_btn:
                    signin_btn.click()
                
                print("[4] Waiting for redirect...")
                page.wait_for_url('**/qbo.intuit.com/app/**', timeout=30000)
                print("[✓] Login successful!")
                
            except Exception as e:
                print(f"[✗] Error: {e}")
                if not headless:
                    input("Press Enter to close...")
                browser.close()
                return False
        
        # Banking page
        print("[5] Going to Banking...")
        page.goto('https://qbo.intuit.com/app/banking', timeout=30000)
        time.sleep(3)
        
        cookies = {c['name']: c['value'] for c in context.cookies() if 'intuit.com' in c.get('domain', '')}
        
        print("-" * 50)
        print(f"Company ID: {cookies.get('qbo.currentcompanyid')}")
        print(f"Has ticket: {'qbo.ticket' in cookies}")
        print(f"Has CSRF:   {'qbo.csrftoken' in cookies}")
        print("-" * 50)
        
        if not headless:
            input("Press Enter to close...")
        
        browser.close()
        return True

if __name__ == '__main__':
    if not QB_USERNAME or not QB_PASSWORD:
        print("Set QB_USERNAME and QB_PASSWORD")
        sys.exit(1)
    
    headless = '--visible' not in sys.argv
    success = test_login(headless=headless)
    sys.exit(0 if success else 1)