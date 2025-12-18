#!/usr/bin/env python3
"""
Test stealth login locally before deploying to Azure
"""

import os
import sys
import time
import random
from playwright.sync_api import sync_playwright

QB_USERNAME = os.getenv('QB_USERNAME', 'it@dispatchenergy.com')
QB_PASSWORD = os.getenv('QB_PASSWORD', '')

def human_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))

def test_stealth_login(headless=True):
    print(f"Testing stealth login for: {QB_USERNAME}")
    print(f"Headless: {headless}")
    print("-" * 50)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-infobars',
                '--window-size=1920,1080',
                '--start-maximized',
            ]
        )
        
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            timezone_id='America/Denver',
            locale='en-US',
            color_scheme='light',
            java_script_enabled=True,
            has_touch=False,
            is_mobile=False,
        )
        
        # Stealth scripts
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            window.chrome = { runtime: {} };
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """)
        
        page = context.new_page()
        
        def human_mouse_move():
            for _ in range(random.randint(2, 5)):
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                page.mouse.move(x, y)
                time.sleep(random.uniform(0.1, 0.3))
        
        print("[1] Navigating to qbo.intuit.com...")
        page.goto('https://qbo.intuit.com', timeout=60000)
        human_delay(3, 5)
        human_mouse_move()
        
        print(f"    URL: {page.url}")
        
        if 'qbo.intuit.com/app/' in page.url:
            print("[✓] Already logged in!")
        else:
            human_mouse_move()
            
            # Check for remembered account
            account_tile = page.query_selector(f'text="{QB_USERNAME}"')
            if account_tile:
                print("[2] Found remembered account - clicking...")
                account_tile.click()
                human_delay(2, 3)
            else:
                print("[2] Entering email with stealth...")
                email_input = page.wait_for_selector(
                    '[data-testid="IdentifierFirstInternationalUserIdInput"]',
                    timeout=15000
                )
                
                # Move mouse to input
                box = email_input.bounding_box()
                if box:
                    page.mouse.move(box['x'] + box['width']/2, box['y'] + box['height']/2)
                    time.sleep(random.uniform(0.2, 0.5))
                
                email_input.click()
                human_delay(0.3, 0.7)
                
                # Type with variable speed
                for char in QB_USERNAME:
                    page.keyboard.type(char, delay=random.randint(50, 150))
                    if random.random() < 0.1:
                        time.sleep(random.uniform(0.1, 0.3))
                
                human_delay(0.5, 1.5)
                human_mouse_move()
                
                print("    Clicking Sign in...")
                signin_btn = page.query_selector('[data-testid="IdentifierFirstSubmitButton"]')
                if signin_btn:
                    box = signin_btn.bounding_box()
                    if box:
                        page.mouse.move(box['x'] + box['width']/2, box['y'] + box['height']/2)
                        time.sleep(random.uniform(0.2, 0.4))
                    signin_btn.click()
                
                human_delay(3, 5)
            
            # Check for CAPTCHA
            page_text = page.inner_text('body').lower() if page.query_selector('body') else ''
            if 'captcha' in page_text or 'robot' in page_text or "i'm not a robot" in page_text:
                print("[!] CAPTCHA detected!")
                if not headless:
                    print("    Solve it manually, then press Enter...")
                    input()
                else:
                    print("[✗] CAPTCHA triggered in headless mode")
                    browser.close()
                    return False
            
            # Password
            print("[3] Entering password with stealth...")
            try:
                password_input = page.wait_for_selector(
                    'input[type="password"]:not([data-testid="SignInHiddenInput"])',
                    timeout=15000
                )
                
                box = password_input.bounding_box()
                if box:
                    page.mouse.move(box['x'] + box['width']/2, box['y'] + box['height']/2)
                    time.sleep(random.uniform(0.2, 0.5))
                
                password_input.click()
                human_delay(0.3, 0.7)
                
                for char in QB_PASSWORD:
                    page.keyboard.type(char, delay=random.randint(50, 150))
                    if random.random() < 0.1:
                        time.sleep(random.uniform(0.1, 0.3))
                
                human_delay(0.5, 1.5)
                human_mouse_move()
                
                signin_btn = page.query_selector('button[type="submit"]')
                if signin_btn:
                    box = signin_btn.bounding_box()
                    if box:
                        page.mouse.move(box['x'] + box['width']/2, box['y'] + box['height']/2)
                        time.sleep(random.uniform(0.2, 0.4))
                    signin_btn.click()
                
                print("[4] Waiting for redirect...")
                page.wait_for_url('**/qbo.intuit.com/app/**', timeout=30000)
                print("[✓] Login successful!")
                
            except Exception as e:
                print(f"[✗] Error: {e}")
                page_text = page.inner_text('body')[:500] if page.query_selector('body') else ''
                print(f"    Page text: {page_text}")
                if not headless:
                    input("Press Enter to close...")
                browser.close()
                return False
        
        # Banking
        print("[5] Going to Banking...")
        page.goto('https://qbo.intuit.com/app/banking', timeout=30000)
        human_delay(3, 5)
        
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
    if not QB_PASSWORD:
        print("Set QB_PASSWORD environment variable")
        sys.exit(1)
    
    headless = '--visible' not in sys.argv
    success = test_stealth_login(headless=headless)
    sys.exit(0 if success else 1)