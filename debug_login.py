#!/usr/bin/env python3
"""
Debug script - inspect the login page to find correct selectors
"""

from playwright.sync_api import sync_playwright
import time

def debug_login_page():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        print("Navigating to QB...")
        page.goto('https://qbo.intuit.com', timeout=60000)
        
        # Just wait a few seconds instead of networkidle
        print("Waiting for page to load...")
        time.sleep(5)
        
        print(f"\nCurrent URL: {page.url}")
        print("\n" + "="*60)
        print("Looking for INPUT elements on page:")
        print("="*60)
        
        # Find all input elements
        inputs = page.query_selector_all('input')
        for i, inp in enumerate(inputs):
            try:
                inp_type = inp.get_attribute('type') or 'text'
                inp_name = inp.get_attribute('name') or ''
                inp_id = inp.get_attribute('id') or ''
                inp_placeholder = inp.get_attribute('placeholder') or ''
                inp_class = inp.get_attribute('class') or ''
                data_testid = inp.get_attribute('data-testid') or ''
                aria_label = inp.get_attribute('aria-label') or ''
                
                print(f"\n[Input {i}]")
                print(f"  type: {inp_type}")
                print(f"  name: {inp_name}")
                print(f"  id: {inp_id}")
                print(f"  placeholder: {inp_placeholder}")
                print(f"  aria-label: {aria_label}")
                print(f"  data-testid: {data_testid}")
                if inp_class:
                    print(f"  class: {inp_class[:80]}")
            except Exception as e:
                print(f"  Error: {e}")
        
        print("\n" + "="*60)
        print("Looking for BUTTON elements:")
        print("="*60)
        
        buttons = page.query_selector_all('button')
        for i, btn in enumerate(buttons):
            try:
                btn_text = btn.inner_text().strip()[:30]
                btn_type = btn.get_attribute('type') or ''
                btn_id = btn.get_attribute('id') or ''
                data_testid = btn.get_attribute('data-testid') or ''
                
                print(f"\n[Button {i}]")
                print(f"  text: '{btn_text}'")
                print(f"  type: {btn_type}")
                print(f"  id: {btn_id}")
                print(f"  data-testid: {data_testid}")
            except Exception as e:
                print(f"  Error: {e}")
        
        print("\n" + "="*60)
        input("Press Enter to close browser...")
        browser.close()

if __name__ == '__main__':
    debug_login_page()