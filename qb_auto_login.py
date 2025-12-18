#!/usr/bin/env python3
"""
QuickBooks Automated Login

Automates the QB login flow using stored credentials.
This eliminates manual intervention for session refresh.

SECURITY NOTE: 
    - Credentials should be stored in environment variables or Azure Key Vault
    - Never commit credentials to version control
    - Use a dedicated service account, not a personal account

Requirements:
    - No 2FA/MFA on the QuickBooks account
    - Credentials stored securely (env vars, Key Vault, etc.)

Usage:
    # Set credentials
    export QB_USERNAME="service-account@company.com"
    export QB_PASSWORD="your-password"
    
    # Run automated login
    python qb_auto_login.py --login
    
    # Or run full flow: login if needed, then scrape
    python qb_auto_login.py --auto-scrape
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# Configuration
DATA_DIR = Path(os.getenv('QB_DATA_DIR', Path.home() / '.qb_bank_scraper'))
SESSION_DIR = Path(os.getenv('QB_SESSION_DIR', DATA_DIR / 'session'))
STATE_FILE = SESSION_DIR / 'state.json'

QB_LOGIN_URL = 'https://qbo.intuit.com'
QB_BANKING_URL = 'https://qbo.intuit.com/app/banking'

# Credentials from environment
QB_USERNAME = os.getenv('QB_USERNAME')
QB_PASSWORD = os.getenv('QB_PASSWORD')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
SESSION_DIR.mkdir(parents=True, exist_ok=True)


class QBAutoLogin:
    """Automated QuickBooks login handler"""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
    
    def __enter__(self):
        self.playwright = sync_playwright().start()
        
        # Use persistent context for more browser-like behavior
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
            ]
        )
        
        # Try to load existing session
        if STATE_FILE.exists():
            try:
                self.context = self.browser.new_context(
                    storage_state=str(STATE_FILE),
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15'
                )
                logger.info("Loaded existing session")
            except Exception as e:
                logger.warning(f"Could not load session: {e}")
                self.context = self.browser.new_context()
        else:
            self.context = self.browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15'
            )
        
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
        """Save current session state"""
        self.context.storage_state(path=str(STATE_FILE))
        logger.info(f"Session saved to {STATE_FILE}")
    
    def is_logged_in(self) -> bool:
        """Check if we're currently logged in"""
        try:
            self.page.goto(QB_BANKING_URL, timeout=30000)
            self.page.wait_for_load_state('networkidle', timeout=15000)
            
            current_url = self.page.url
            
            # If we're on the banking page or dashboard, we're logged in
            if '/app/' in current_url and 'signin' not in current_url.lower():
                logger.info(f"Already logged in - URL: {current_url}")
                return True
            
            # If we got redirected to login, we're not logged in
            if 'accounts.intuit.com' in current_url or 'signin' in current_url.lower():
                logger.info("Not logged in - redirected to login page")
                return False
            
            logger.warning(f"Unknown state - URL: {current_url}")
            return False
            
        except Exception as e:
            logger.error(f"Error checking login status: {e}")
            return False
    
    def login(self, username: str = None, password: str = None) -> bool:
        """
        Perform automated login to QuickBooks.
        
        Returns True if login successful, False otherwise.
        """
        username = username or QB_USERNAME
        password = password or QB_PASSWORD
        
        if not username or not password:
            logger.error("QB_USERNAME and QB_PASSWORD environment variables required")
            return False
        
        logger.info("Starting automated login...")
        
        try:
            # Navigate to QB
            self.page.goto(QB_LOGIN_URL, timeout=30000)
            self.page.wait_for_load_state('networkidle', timeout=15000)
            
            # Check if we're already logged in
            if '/app/' in self.page.url and 'signin' not in self.page.url.lower():
                logger.info("Already logged in!")
                self.save_session()
                return True
            
            # Wait for login form
            logger.info("Waiting for login form...")
            
            # Intuit login flow - they use a unified sign-in
            # First, enter email/username
            email_selectors = [
                'input[name="Email"]',
                'input[id="ius-identifier"]',
                'input[id="ius-userid"]',
                'input[type="email"]',
                '#Email',
            ]
            
            email_input = None
            for selector in email_selectors:
                try:
                    email_input = self.page.wait_for_selector(selector, timeout=5000)
                    if email_input:
                        break
                except:
                    continue
            
            if not email_input:
                logger.error("Could not find email input field")
                self._save_debug_screenshot("login_no_email_field")
                return False
            
            # Enter email
            logger.info("Entering username...")
            email_input.fill(username)
            
            # Click continue/next button
            continue_selectors = [
                'button[type="submit"]',
                'button:has-text("Continue")',
                'button:has-text("Sign in")',
                'button:has-text("Next")',
                '#ius-sign-in-submit-btn',
            ]
            
            for selector in continue_selectors:
                try:
                    btn = self.page.query_selector(selector)
                    if btn and btn.is_visible():
                        btn.click()
                        break
                except:
                    continue
            
            # Wait for password field (might be on same page or next page)
            time.sleep(2)  # Brief pause for page transition
            
            password_selectors = [
                'input[name="Password"]',
                'input[id="ius-password"]',
                'input[type="password"]',
                '#Password',
            ]
            
            password_input = None
            for selector in password_selectors:
                try:
                    password_input = self.page.wait_for_selector(selector, timeout=10000)
                    if password_input:
                        break
                except:
                    continue
            
            if not password_input:
                logger.error("Could not find password input field")
                self._save_debug_screenshot("login_no_password_field")
                return False
            
            # Enter password
            logger.info("Entering password...")
            password_input.fill(password)
            
            # Click sign in
            time.sleep(0.5)  # Brief pause
            
            signin_selectors = [
                'button[type="submit"]',
                'button:has-text("Sign in")',
                'button:has-text("Continue")',
                '#ius-sign-in-submit-btn',
            ]
            
            for selector in signin_selectors:
                try:
                    btn = self.page.query_selector(selector)
                    if btn and btn.is_visible():
                        btn.click()
                        break
                except:
                    continue
            
            # Wait for redirect to QB dashboard
            logger.info("Waiting for login to complete...")
            
            try:
                # Wait for either success (QB app) or failure (still on login)
                self.page.wait_for_url('**/app/**', timeout=30000)
                logger.info("Login successful!")
                
                # Navigate to banking to ensure full session
                self.page.goto(QB_BANKING_URL, timeout=30000)
                self.page.wait_for_load_state('networkidle', timeout=15000)
                
                # Save session
                self.save_session()
                return True
                
            except PlaywrightTimeout:
                # Check if we hit 2FA or error
                current_url = self.page.url
                page_text = self.page.content().lower()
                
                if 'verification' in page_text or 'security code' in page_text or '2fa' in page_text:
                    logger.error("2FA/MFA detected - automated login not possible")
                    self._save_debug_screenshot("login_2fa_required")
                    return False
                
                if 'incorrect' in page_text or 'invalid' in page_text or 'wrong' in page_text:
                    logger.error("Invalid credentials")
                    return False
                
                logger.error(f"Login timed out. Current URL: {current_url}")
                self._save_debug_screenshot("login_timeout")
                return False
        
        except Exception as e:
            logger.exception(f"Login failed with error: {e}")
            self._save_debug_screenshot("login_error")
            return False
    
    def _save_debug_screenshot(self, name: str):
        """Save screenshot for debugging"""
        try:
            screenshot_path = DATA_DIR / f"debug_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            self.page.screenshot(path=str(screenshot_path))
            logger.info(f"Debug screenshot saved: {screenshot_path}")
        except:
            pass
    
    def ensure_logged_in(self, username: str = None, password: str = None) -> bool:
        """
        Ensure we have a valid session - login if needed.
        
        Returns True if we have a valid session, False if login failed.
        """
        if self.is_logged_in():
            # Refresh the session by saving current state
            self.save_session()
            return True
        
        logger.info("Session invalid - performing login...")
        return self.login(username, password)


def auto_login(headless: bool = True) -> bool:
    """Automated login flow"""
    with QBAutoLogin(headless=headless) as qb:
        return qb.ensure_logged_in()


def auto_scrape(headless: bool = True) -> dict:
    """
    Full automated flow: login if needed, then scrape.
    
    Returns dict with results.
    """
    # First, ensure we're logged in
    with QBAutoLogin(headless=headless) as qb:
        if not qb.ensure_logged_in():
            return {'success': False, 'error': 'Login failed'}
    
    # Now use the HTTP scraper with the fresh session
    from qb_http_scraper import QBBankScraper, QuickBaseSync
    
    try:
        scraper = QBBankScraper()
        
        # Verify session works
        if not scraper.check_session():
            return {'success': False, 'error': 'Session check failed after login'}
        
        # Scrape
        accounts, transactions = scraper.scrape_all_pending()
        
        return {
            'success': True,
            'accounts': len(accounts),
            'transactions': len(transactions),
            'data': {
                'accounts': [a.__dict__ for a in accounts],
                'transactions': [t.__dict__ for t in transactions]
            }
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}


def main():
    parser = argparse.ArgumentParser(description='QuickBooks Automated Login')
    parser.add_argument('--login', action='store_true', help='Perform automated login')
    parser.add_argument('--check', action='store_true', help='Check if currently logged in')
    parser.add_argument('--auto-scrape', action='store_true', help='Login if needed, then scrape')
    parser.add_argument('--headless', action='store_true', help='Run browser headless')
    parser.add_argument('--visible', action='store_true', help='Run browser visible (for debugging)')
    
    args = parser.parse_args()
    
    # Default to headless unless --visible specified
    headless = not args.visible
    
    if not any([args.login, args.check, args.auto_scrape]):
        parser.print_help()
        print("\n" + "="*60)
        print("SETUP:")
        print("="*60)
        print("export QB_USERNAME='your-email@company.com'")
        print("export QB_PASSWORD='your-password'")
        print("")
        print("Then run:")
        print("  python qb_auto_login.py --login --visible  # Test login visibly")
        print("  python qb_auto_login.py --login            # Headless login")
        print("  python qb_auto_login.py --auto-scrape      # Full flow")
        print("="*60)
        return
    
    if args.check:
        with QBAutoLogin(headless=headless) as qb:
            if qb.is_logged_in():
                print("✓ Currently logged in")
                sys.exit(0)
            else:
                print("✗ Not logged in")
                sys.exit(1)
    
    if args.login:
        success = auto_login(headless=headless)
        if success:
            print("✓ Login successful - session saved")
            sys.exit(0)
        else:
            print("✗ Login failed")
            sys.exit(1)
    
    if args.auto_scrape:
        result = auto_scrape(headless=headless)
        
        if result['success']:
            print(f"✓ Scrape complete: {result['accounts']} accounts, {result['transactions']} transactions")
            
            # Save output
            output_file = DATA_DIR / 'pending_transactions.json'
            with open(output_file, 'w') as f:
                json.dump(result['data'], f, indent=2, default=str)
            print(f"  Saved to: {output_file}")
            
            sys.exit(0)
        else:
            print(f"✗ Failed: {result['error']}")
            sys.exit(1)


if __name__ == '__main__':
    main()
