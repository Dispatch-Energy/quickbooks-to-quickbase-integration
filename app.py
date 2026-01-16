"""
QuickBooks → QuickBase Sync - Container App (v2.1)

FastAPI app with endpoints:
- POST /sync - trigger full sync (bank feeds + GL)
    - refresh_feeds=true: clicks Update button, runs GL during wait
- POST /sync-gl - trigger GL sync (OAuth-based)
- POST /sync-all - alias for /sync
- POST /code - submit SMS verification code (manual)
- POST /twilio/sms - Twilio webhook for automatic SMS code capture
- POST /telnyx/sms - Telnyx webhook for automatic SMS code capture
- GET /screenshot - view latest screenshot
- GET /health - health check
"""

import os
import re
import time
import random
import logging
import threading
from datetime import datetime, timezone, date
from typing import Dict, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Form, Request
from fastapi.responses import Response, PlainTextResponse
from pydantic import BaseModel
from playwright.sync_api import sync_playwright
import requests

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

QB_USERNAME = os.getenv('QB_USERNAME')
QB_PASSWORD = os.getenv('QB_PASSWORD')
QB_API_KEY = 'prdakyresxaDrhFXaSARXaUdj1S8M7h6YK7YGekc'
QB_BASE_URL = 'https://qbo.intuit.com'

QUICKBASE_REALM = os.getenv('QUICKBASE_REALM', 'dispatchenergy')
QUICKBASE_TOKEN = os.getenv('QUICKBASE_TOKEN')

# Bank feeds tables
ACCOUNTS_TABLE_ID = os.getenv('ACCOUNTS_TABLE_ID')
TRANSACTIONS_TABLE_ID = os.getenv('TRANSACTIONS_TABLE_ID')
BALANCES_TABLE_ID = os.getenv('BALANCES_TABLE_ID')  # NEW: Bank Balance table

# Bank Account field mappings
ACCOUNT_FIELDS = {
    'quickbooks_id': 6, 'account_name': 7, 'nickname': 8, 'institution': 9,
    'type': 10, 'balance': 11, 'qb_balance': 12, 'pending_txns': 13,
    'last_updated': 14, 'last_synced': 15,
}

# Bank Transaction field mappings
TRANSACTION_FIELDS = {
    'quickbooks_id': 6, 'internal_id': 7, 'date': 8, 'description': 9,
    'amount': 10, 'type': 11, 'merchant_name': 12, 'related_account': 13,
}

# Bank Balance field mappings (NEW)
BALANCE_FIELDS = {
    'balance': 6,            # Currency
    'date_added': 7,         # Date
    'related_account': 8,    # Reference to Bank Account
}

# Bank Feed Refresh Settings
REFRESH_POLL_INTERVAL = 15  # seconds between status checks
REFRESH_TIMEOUT = 600       # max wait time (10 minutes)
REFRESH_BROWSER_TIMEOUT = 600  # max wait for browser-based refresh (10 minutes)

# =============================================================================
# In-memory state
# =============================================================================

class AppState:
    pending_sms_code: Optional[str] = None
    latest_screenshot: Optional[bytes] = None
    screenshot_timestamp: Optional[str] = None
    sync_in_progress: bool = False
    last_sync_result: Optional[str] = None

state = AppState()

# =============================================================================
# FastAPI App
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("QB Sync Container App v2 starting...")
    yield
    logger.info("Shutting down...")

app = FastAPI(title="QB Sync v2", lifespan=lifespan)


class CodeRequest(BaseModel):
    sms_code: str


class SyncRequest(BaseModel):
    skip_balances: bool = False
    skip_transactions: bool = False
    refresh_feeds: bool = False  # Trigger bank feed update before scraping
    refresh_timeout: int = REFRESH_TIMEOUT  # Max seconds to wait for refresh


@app.get("/health")
def health_check():
    return {
        "status": "ok", 
        "sync_in_progress": state.sync_in_progress,
        "version": "2.1",
        "features": ["bank_feeds", "bank_balances", "bank_feed_refresh", "gl_sync"]
    }


@app.post("/code")
def submit_code(req: CodeRequest):
    """Submit SMS verification code."""
    code = req.sms_code.strip()
    if not code or len(code) != 6 or not code.isdigit():
        raise HTTPException(400, "Invalid code - must be 6 digits")
    
    state.pending_sms_code = code
    logger.info(f"SMS code received: {code[:2]}****")
    return {"status": "received", "message": "Code stored - sync will continue"}


@app.post("/twilio/sms")
async def twilio_sms_webhook(
    From: str = Form(...),
    Body: str = Form(...),
    To: str = Form(None),
    MessageSid: str = Form(None),
):
    """
    Twilio webhook for incoming SMS.
    Automatically extracts verification code from Intuit SMS messages.
    """
    logger.info(f"Twilio SMS received from {From}: {Body[:50]}...")
    
    # Extract 6-digit code from message body
    # Intuit messages typically contain: "Your Intuit verification code is 123456"
    code_match = re.search(r'\b(\d{6})\b', Body)
    
    if code_match:
        code = code_match.group(1)
        state.pending_sms_code = code
        logger.info(f"Extracted verification code: {code[:2]}****")
        
        # Return TwiML response (empty response = no reply SMS)
        return PlainTextResponse(
            content='<?xml version="1.0" encoding="UTF-8"?><Response></Response>',
            media_type="application/xml"
        )
    else:
        logger.warning(f"No 6-digit code found in SMS: {Body}")
        return PlainTextResponse(
            content='<?xml version="1.0" encoding="UTF-8"?><Response></Response>',
            media_type="application/xml"
        )


@app.post("/telnyx/sms")
async def telnyx_sms_webhook(request: Request):
    """
    Telnyx webhook for incoming SMS.
    Automatically extracts verification code from Intuit SMS messages.
    
    Telnyx sends JSON payload:
    {
      "data": {
        "event_type": "message.received",
        "payload": {
          "from": {"phone_number": "+15551234567"},
          "text": "Your Intuit verification code is 123456"
        }
      }
    }
    """
    try:
        body = await request.json()
        
        # Extract the relevant data from Telnyx format
        data = body.get('data', {})
        event_type = data.get('event_type', '')
        payload = data.get('payload', {})
        
        from_number = payload.get('from', {}).get('phone_number', 'unknown')
        text = payload.get('text', '')
        
        logger.info(f"Telnyx SMS received from {from_number}: {text[:50]}...")
        
        # Only process inbound messages
        if event_type != 'message.received':
            logger.info(f"Ignoring Telnyx event type: {event_type}")
            return {"status": "ignored", "event_type": event_type}
        
        # Extract 6-digit code from message body
        code_match = re.search(r'\b(\d{6})\b', text)
        
        if code_match:
            code = code_match.group(1)
            state.pending_sms_code = code
            logger.info(f"Extracted verification code: {code[:2]}****")
            return {"status": "received", "code_extracted": True}
        else:
            logger.warning(f"No 6-digit code found in SMS: {text}")
            return {"status": "received", "code_extracted": False}
            
    except Exception as e:
        logger.error(f"Error processing Telnyx webhook: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/screenshot")
def get_screenshot():
    """Get latest screenshot."""
    if not state.latest_screenshot:
        raise HTTPException(404, "No screenshot available")
    
    return Response(
        content=state.latest_screenshot,
        media_type="image/png",
        headers={"X-Timestamp": state.screenshot_timestamp or "unknown"}
    )


@app.post("/sync")
def trigger_sync(req: SyncRequest = None):
    """
    Trigger full sync (bank feeds + GL).
    
    With refresh_feeds=True:
    1. Login to QuickBooks
    2. Click Update button and wait for completion (browser-based)
    3. Run GL sync during the wait
    4. Scrape and sync bank data
    
    Without refresh_feeds (default):
    1. Login and scrape bank feeds
    2. Sync to QuickBase
    3. Run GL sync
    """
    if state.sync_in_progress:
        return {"status": "already_running", "message": "Sync already in progress"}
    
    # Validate config
    missing = []
    if not QB_USERNAME: missing.append('QB_USERNAME')
    if not QB_PASSWORD: missing.append('QB_PASSWORD')
    if not QUICKBASE_TOKEN: missing.append('QUICKBASE_TOKEN')
    if not ACCOUNTS_TABLE_ID: missing.append('ACCOUNTS_TABLE_ID')
    if not TRANSACTIONS_TABLE_ID: missing.append('TRANSACTIONS_TABLE_ID')
    
    if missing:
        raise HTTPException(500, f"Missing config: {', '.join(missing)}")
    
    try:
        state.sync_in_progress = True
        state.pending_sms_code = None
        
        skip_balances = req.skip_balances if req else False
        skip_transactions = req.skip_transactions if req else False
        refresh_feeds = req.refresh_feeds if req else False
        refresh_timeout = req.refresh_timeout if req else REFRESH_BROWSER_TIMEOUT
        
        gl_result = "not run"
        
        if refresh_feeds:
            # Browser-based refresh: login, click Update, wait for completion
            print("Step 1: Login and refresh bank feeds (browser-based)...", flush=True)
            
            # Start GL sync in parallel using threading
            import threading
            gl_thread = None
            gl_result_container = {"result": "not run"}
            
            def run_gl_in_background():
                try:
                    gl_result_container["result"] = run_gl_sync()
                except Exception as e:
                    gl_result_container["result"] = f"error: {e}"
            
            # Start GL sync in background thread
            print("Starting GL sync in background...", flush=True)
            gl_thread = threading.Thread(target=run_gl_in_background)
            gl_thread.start()
            
            # Do browser-based login and refresh (this takes ~8 minutes)
            cookies = auto_login_and_refresh(timeout=refresh_timeout)
            
            # Wait for GL sync to complete
            if gl_thread:
                print("Waiting for GL sync to complete...", flush=True)
                gl_thread.join(timeout=60)  # Max 60s extra wait
                gl_result = gl_result_container["result"]
                print(f"GL sync result: {gl_result}", flush=True)
        else:
            # Normal mode: just login
            print("Step 1: Logging in to QuickBooks...", flush=True)
            cookies = auto_login()
        
        print(f"Logged in, company_id={cookies.get('qbo.currentcompanyid')}", flush=True)
        
        # Scrape and sync bank feeds
        print("Step 2: Scraping QuickBooks...", flush=True)
        accounts, transactions = scrape_quickbooks(cookies)
        print(f"Scraped {len(accounts)} accounts, {len(transactions)} transactions", flush=True)
        
        print("Step 3: Syncing accounts to QuickBase...", flush=True)
        account_map = sync_accounts(accounts)
        
        # Sync bank balances
        if not skip_balances and BALANCES_TABLE_ID:
            print("Step 4: Syncing bank balances...", flush=True)
            balance_result = sync_bank_balances(accounts, account_map)
        else:
            balance_result = "skipped"
        
        # Sync transactions
        if not skip_transactions:
            print("Step 5: Syncing transactions...", flush=True)
            sync_transactions(transactions, account_map)
        
        bank_result = f"{len(accounts)} accounts, {len(transactions)} txns, balances: {balance_result}"
        
        # Run GL sync if not already done during refresh
        if gl_result == "not run":
            print("Step 6: Running GL sync...", flush=True)
            gl_result = run_gl_sync()
        
        result = f"Bank feeds: {bank_result} | GL: {gl_result}"
        state.last_sync_result = result
        return {"status": "complete", "result": result}
        
    except Exception as e:
        error_msg = str(e)
        state.last_sync_result = f"Error: {error_msg}"
        
        if "SMS_VERIFICATION_TIMEOUT" in error_msg:
            raise HTTPException(408, "Verification timeout - no code received within 3 minutes")
        elif "SMS_VERIFICATION_REQUIRED" in error_msg:
            raise HTTPException(202, "SMS sent - POST to /code with {\"sms_code\": \"123456\"}")
        else:
            raise HTTPException(500, error_msg)
    finally:
        state.sync_in_progress = False


@app.post("/sync-gl")
def trigger_gl_sync():
    """Trigger GL sync (OAuth-based, for accounting data)."""
    if state.sync_in_progress:
        return {"status": "already_running", "message": "Sync already in progress"}
    
    try:
        state.sync_in_progress = True
        result = run_gl_sync()
        state.last_sync_result = result
        return {"status": "complete", "result": result}
    except Exception as e:
        error_msg = str(e)
        state.last_sync_result = f"Error: {error_msg}"
        raise HTTPException(500, error_msg)
    finally:
        state.sync_in_progress = False


@app.post("/sync-all")
def trigger_full_sync(req: SyncRequest = None):
    """
    Trigger both bank feeds and GL sync.
    Alias for /sync - kept for backwards compatibility.
    """
    return trigger_sync(req)


# =============================================================================
# Helper Functions
# =============================================================================

def human_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))


def save_screenshot(screenshot_bytes: bytes):
    """Save screenshot to state."""
    state.latest_screenshot = screenshot_bytes
    state.screenshot_timestamp = datetime.now().isoformat()
    logger.info(f"Screenshot saved at {state.screenshot_timestamp}")


def get_qb_headers(cookies: Dict[str, str]) -> tuple:
    """Build standard QuickBooks API headers."""
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
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
    }
    
    if cookies.get('qbo.csrftoken'):
        headers['Csrftoken'] = cookies['qbo.csrftoken']
    
    if cookies.get('qbo.xcsrfderivationkey'):
        headers['x-csrf-token'] = cookies['qbo.xcsrfderivationkey']
    
    return headers, company_id


def trigger_bank_update(cookies: Dict[str, str]) -> Optional[dict]:
    """
    Trigger QuickBooks bank feed update (equivalent to clicking "Update" button).
    Returns the initial job status response or None on failure.
    """
    import uuid
    
    logger.info("Triggering bank feed update...")
    print("Triggering bank feed update (Update button)...", flush=True)
    
    headers, company_id = get_qb_headers(cookies)
    
    # Add required headers from captured request
    headers['intuit-plugin-id'] = 'integrations-datain-ui'
    headers['intuit_tid'] = str(uuid.uuid4())
    
    if cookies.get('qbo.authid'):
        headers['intuit-user-id'] = cookies['qbo.authid']
    elif cookies.get('qbo.gauthid'):
        headers['intuit-user-id'] = cookies['qbo.gauthid']
    
    try:
        # The actual request body from browser capture
        body = {"fiList": []}
        
        resp = requests.post(
            f'{QB_BASE_URL}/api/neo/v2/company/{company_id}/olb/manualUpdate/start',
            headers=headers,
            json=body,
            timeout=30
        )
        
        if resp.status_code != 200:
            logger.error(f"Failed to start update: {resp.status_code}")
            logger.error(f"Response: {resp.text[:500]}")
            print(f"Failed to start bank feed update: {resp.status_code}", flush=True)
            print(f"Response: {resp.text[:300]}", flush=True)
            return None
        
        result = resp.json()
        
        # Count accounts being updated
        total_accounts = 0
        banks = []
        for sub_job in result.get('subJobs', []):
            fi_name = sub_job.get('fiName', 'Unknown')
            num_accounts = len(sub_job.get('accounts', []))
            total_accounts += num_accounts
            banks.append(fi_name)
        
        logger.info(f"Update started: {len(banks)} banks, {total_accounts} accounts")
        print(f"Bank feed update started: {len(banks)} banks, {total_accounts} accounts", flush=True)
        
        return result
        
    except Exception as e:
        logger.error(f"Error triggering bank update: {e}")
        print(f"Error triggering bank update: {e}", flush=True)
        return None


def poll_update_status(cookies: Dict[str, str], timeout: int = REFRESH_TIMEOUT) -> bool:
    """
    Poll for bank feed update completion.
    Returns True if update completed successfully.
    """
    import uuid
    
    headers, company_id = get_qb_headers(cookies)
    
    # Add required headers
    headers['intuit-plugin-id'] = 'integrations-datain-ui'
    if cookies.get('qbo.authid'):
        headers['intuit-user-id'] = cookies['qbo.authid']
    elif cookies.get('qbo.gauthid'):
        headers['intuit-user-id'] = cookies['qbo.gauthid']
    
    logger.info(f"Polling for bank feed update completion (timeout: {timeout}s)...")
    print(f"Waiting for bank feed update to complete (timeout: {timeout}s)...", flush=True)
    
    start_time = time.time()
    last_status = {}
    
    # The actual request body from browser capture
    body = {"fiList": []}
    
    while time.time() - start_time < timeout:
        try:
            # Generate new transaction ID for each poll
            headers['intuit_tid'] = str(uuid.uuid4())
            
            # Get status by calling start again (returns current state)
            resp = requests.post(
                f'{QB_BASE_URL}/api/neo/v2/company/{company_id}/olb/manualUpdate/start',
                headers=headers,
                json=body,
                timeout=30
            )
            
            if resp.status_code == 200:
                result = resp.json()
                is_complete = result.get('isComplete', False)
                has_errors = result.get('hasErrors', False)
                
                # Count completed sub-jobs
                completed = sum(1 for sj in result.get('subJobs', []) if sj.get('isComplete', False))
                total = len(result.get('subJobs', []))
                errors = sum(1 for sj in result.get('subJobs', []) if sj.get('hasError', False))
                
                # Print progress if changed
                status_key = (completed, total, errors)
                if status_key != last_status:
                    elapsed = int(time.time() - start_time)
                    status_msg = f"[{elapsed}s] Bank feed progress: {completed}/{total} banks complete"
                    if errors:
                        status_msg += f" ({errors} with errors)"
                    logger.info(status_msg)
                    print(status_msg, flush=True)
                    last_status = status_key
                
                if is_complete:
                    elapsed = int(time.time() - start_time)
                    logger.info(f"Bank feed update complete! ({elapsed}s)")
                    print(f"Bank feed update complete! ({elapsed}s)", flush=True)
                    
                    if has_errors:
                        error_banks = [sj.get('fiName') for sj in result.get('subJobs', []) if sj.get('hasError')]
                        logger.warning(f"Some banks had errors: {error_banks}")
                        print(f"Warning: Some banks had errors: {error_banks}", flush=True)
                    
                    return True
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"Status check failed: {e}")
        
        time.sleep(REFRESH_POLL_INTERVAL)
    
    logger.error(f"Bank feed update timed out after {timeout}s")
    print(f"Bank feed update timed out after {timeout}s", flush=True)
    return False


# =============================================================================
# Bank Feeds Sync
# =============================================================================

def run_bank_feeds_sync(skip_balances: bool = False, skip_transactions: bool = False) -> str:
    """Run the full bank feeds sync flow."""
    logger.info("Starting bank feeds sync...")
    print("=== BANK FEEDS SYNC STARTED ===", flush=True)
    start_time = time.time()
    
    try:
        print("Step 1: Logging in...", flush=True)
        cookies = auto_login()
        print(f"Step 2: Got cookies, company_id={cookies.get('qbo.currentcompanyid')}", flush=True)
        
        print("Step 3: Scraping QuickBooks...", flush=True)
        accounts, transactions = scrape_quickbooks(cookies)
        print(f"Step 4: Scraped {len(accounts)} accounts, {len(transactions)} transactions", flush=True)
        
        print("Step 5: Syncing accounts to QuickBase...", flush=True)
        account_map = sync_accounts(accounts)
        print(f"Step 6: Account map has {len(account_map)} entries", flush=True)
        
        # NEW: Sync bank balances
        if not skip_balances and BALANCES_TABLE_ID:
            print("Step 7: Syncing bank balances (daily snapshot)...", flush=True)
            balance_result = sync_bank_balances(accounts, account_map)
            print(f"Step 7 complete: {balance_result}", flush=True)
        else:
            print("Step 7: Skipping bank balances", flush=True)
            balance_result = "skipped"
        
        if not skip_transactions:
            print("Step 8: Syncing transactions...", flush=True)
            sync_transactions(transactions, account_map)
            print("Step 8 complete!", flush=True)
        else:
            print("Step 8: Skipping transactions", flush=True)
        
        elapsed = time.time() - start_time
        result = f"{len(accounts)} accounts, {len(transactions)} txns, balances: {balance_result} ({elapsed:.1f}s)"
        logger.info(f"Bank feeds sync complete: {result}")
        print(f"=== BANK FEEDS SYNC COMPLETE: {result} ===", flush=True)
        return result
        
    except Exception as e:
        print(f"=== BANK FEEDS SYNC ERROR: {e} ===", flush=True)
        logger.error(f"Bank feeds sync error: {e}")
        raise


def auto_login() -> Dict[str, str]:
    """Login to QuickBooks via Playwright."""
    logger.info("Starting Playwright login...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-web-security',
                '--window-size=1920,1080',
            ]
        )
        
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            timezone_id='America/Denver',
            locale='en-US',
        )
        
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            window.chrome = { runtime: {} };
        """)
        
        page = context.new_page()
        
        def mouse_move():
            for _ in range(random.randint(2, 4)):
                page.mouse.move(random.randint(100, 800), random.randint(100, 600))
                time.sleep(random.uniform(0.1, 0.2))
        
        logger.info("Navigating to QuickBooks...")
        page.goto('https://qbo.intuit.com', timeout=60000)
        human_delay(3, 5)
        mouse_move()
        
        if 'qbo.intuit.com/app/' not in page.url:
            logger.info(f"On login page: {page.url}")
            mouse_move()
            
            # Email
            account_tile = page.query_selector(f'text="{QB_USERNAME}"')
            if account_tile:
                logger.info("Clicking remembered account...")
                account_tile.click()
                human_delay(2, 3)
            else:
                logger.info("Entering email...")
                email_input = page.wait_for_selector(
                    '[data-testid="IdentifierFirstInternationalUserIdInput"]',
                    timeout=15000
                )
                email_input.click()
                human_delay(0.3, 0.7)
                for char in QB_USERNAME:
                    page.keyboard.type(char, delay=random.randint(50, 150))
                human_delay(0.5, 1)
                
                signin_btn = page.query_selector('[data-testid="IdentifierFirstSubmitButton"]')
                if signin_btn:
                    signin_btn.click()
                human_delay(3, 5)
            
            # Password
            logger.info("Entering password...")
            try:
                password_input = page.wait_for_selector(
                    'input[type="password"]:not([data-testid="SignInHiddenInput"])',
                    timeout=15000
                )
            except:
                page_text = page.inner_text('body')[:500] if page.query_selector('body') else ''
                save_screenshot(page.screenshot())
                raise Exception(f"Password field not found. Page: {page_text[:200]}")
            
            password_input.click()
            human_delay(0.3, 0.7)
            for char in QB_PASSWORD:
                page.keyboard.type(char, delay=random.randint(50, 150))
            human_delay(0.5, 1)
            
            signin_btn = page.query_selector('button[type="submit"]')
            if signin_btn:
                signin_btn.click()
            
            human_delay(3, 5)
            
            # Check result
            try:
                page.wait_for_url('**/qbo.intuit.com/app/**', timeout=15000)
                logger.info("Login successful!")
            except:
                save_screenshot(page.screenshot())
                page_text = page.inner_text('body').lower() if page.query_selector('body') else ''
                
                # Verification screen?
                if 'verify' in page_text or 'check your text' in page_text or 'verification code' in page_text:
                    logger.info("Verification screen detected!")
                    
                    # Click "Text a code" if needed
                    if 'verify it' in page_text and 'text a code' in page_text:
                        text_btn = page.query_selector('text="Text a code"')
                        if text_btn:
                            logger.info("Clicking 'Text a code'...")
                            text_btn.click()
                            human_delay(2, 3)
                            save_screenshot(page.screenshot())
                    
                    # Wait for code
                    logger.info("Waiting for SMS code (3 min timeout)...")
                    logger.info("POST to /code with {\"sms_code\": \"123456\"}")
                    
                    state.pending_sms_code = None
                    poll_start = time.time()
                    timeout = 180
                    
                    while time.time() - poll_start < timeout:
                        if state.pending_sms_code:
                            sms_code = state.pending_sms_code
                            state.pending_sms_code = None
                            logger.info(f"Got code: {sms_code[:2]}****")
                            
                            # Enter code
                            code_input = page.query_selector('input[type="text"], input[type="tel"]')
                            if code_input:
                                code_input.click()
                                human_delay(0.3, 0.5)
                                for char in sms_code:
                                    page.keyboard.type(char, delay=random.randint(80, 150))
                                human_delay(0.5, 1)
                                
                                continue_btn = page.query_selector('button:has-text("Continue"), button[type="submit"]')
                                if continue_btn:
                                    continue_btn.click()
                                    human_delay(3, 5)
                                
                                try:
                                    page.wait_for_url('**/qbo.intuit.com/app/**', timeout=60000)
                                    logger.info("Verification successful!")
                                    break
                                except:
                                    # Check if we're actually logged in despite timeout
                                    save_screenshot(page.screenshot())
                                    current_url = page.url
                                    if 'qbo.intuit.com/app/' in current_url:
                                        logger.info(f"Verification successful (fallback check): {current_url}")
                                        break
                                    else:
                                        logger.error(f"Verification failed. Current URL: {current_url}")
                                        raise Exception("Verification failed after entering code")
                            else:
                                raise Exception("Could not find code input field")
                        
                        time.sleep(2)
                    else:
                        raise Exception("SMS_VERIFICATION_TIMEOUT")
                
                elif 'captcha' in page_text or 'robot' in page_text:
                    raise Exception("CAPTCHA detected - wait and retry later")
                else:
                    raise Exception(f"Login stuck at {page.url}")
        
        # Go to banking
        human_delay(2, 3)
        page.goto('https://qbo.intuit.com/app/banking', timeout=30000)
        human_delay(3, 5)
        
        # Get cookies
        cookies = {c['name']: c['value'] for c in context.cookies() if 'intuit.com' in c.get('domain', '')}
        logger.info(f"Got session for company: {cookies.get('qbo.currentcompanyid')}")
        
        browser.close()
        return cookies


def auto_login_and_refresh(timeout: int = REFRESH_BROWSER_TIMEOUT) -> Dict[str, str]:
    """
    Login to QuickBooks, trigger bank feed refresh via UI, and wait for completion.
    Uses browser DOM watching instead of API polling for reliability.
    Returns cookies when done.
    """
    logger.info("Starting browser-based login and refresh...")
    print("=== BROWSER-BASED BANK FEED REFRESH ===", flush=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-web-security',
                '--window-size=1920,1080',
            ]
        )
        
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            timezone_id='America/Denver',
            locale='en-US',
        )
        
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            window.chrome = { runtime: {} };
        """)
        
        page = context.new_page()
        
        def mouse_move():
            for _ in range(random.randint(2, 4)):
                page.mouse.move(random.randint(100, 800), random.randint(100, 600))
                time.sleep(random.uniform(0.1, 0.2))
        
        # Login flow (same as auto_login)
        logger.info("Navigating to QuickBooks...")
        page.goto('https://qbo.intuit.com', timeout=60000)
        human_delay(3, 5)
        mouse_move()
        
        if 'qbo.intuit.com/app/' not in page.url:
            logger.info(f"On login page: {page.url}")
            mouse_move()
            
            # Email
            account_tile = page.query_selector(f'text="{QB_USERNAME}"')
            if account_tile:
                logger.info("Clicking remembered account...")
                account_tile.click()
                human_delay(2, 3)
            else:
                logger.info("Entering email...")
                email_input = page.wait_for_selector(
                    '[data-testid="IdentifierFirstInternationalUserIdInput"]',
                    timeout=15000
                )
                email_input.click()
                human_delay(0.3, 0.7)
                for char in QB_USERNAME:
                    page.keyboard.type(char, delay=random.randint(50, 150))
                human_delay(0.5, 1)
                
                signin_btn = page.query_selector('[data-testid="IdentifierFirstSubmitButton"]')
                if signin_btn:
                    signin_btn.click()
                human_delay(3, 5)
            
            # Password
            logger.info("Entering password...")
            try:
                password_input = page.wait_for_selector(
                    'input[type="password"]:not([data-testid="SignInHiddenInput"])',
                    timeout=15000
                )
            except:
                page_text = page.inner_text('body')[:500] if page.query_selector('body') else ''
                save_screenshot(page.screenshot())
                raise Exception(f"Password field not found. Page: {page_text[:200]}")
            
            password_input.click()
            human_delay(0.3, 0.7)
            for char in QB_PASSWORD:
                page.keyboard.type(char, delay=random.randint(50, 150))
            human_delay(0.5, 1)
            
            signin_btn = page.query_selector('button[type="submit"]')
            if signin_btn:
                signin_btn.click()
            
            human_delay(3, 5)
            
            # Check result - handle SMS verification same as auto_login
            try:
                page.wait_for_url('**/qbo.intuit.com/app/**', timeout=15000)
            except:
                page_text = page.inner_text('body')[:1000] if page.query_selector('body') else ''
                
                if 'verification code' in page_text.lower() or 'text message' in page_text.lower():
                    logger.info("SMS verification required")
                    save_screenshot(page.screenshot())
                    
                    sms_input = page.query_selector('input[type="tel"], input[inputmode="numeric"]')
                    if sms_input:
                        logger.info("Waiting for SMS code (up to 3 minutes)...")
                        start_wait = time.time()
                        while time.time() - start_wait < 180:
                            if state.pending_sms_code:
                                code = state.pending_sms_code
                                state.pending_sms_code = None
                                logger.info(f"Entering code: {code[:2]}****")
                                
                                sms_input.click()
                                human_delay(0.3, 0.5)
                                page.keyboard.type(code, delay=100)
                                human_delay(0.5, 1)
                                
                                verify_btn = page.query_selector('button[type="submit"]')
                                if verify_btn:
                                    verify_btn.click()
                                
                                try:
                                    page.wait_for_url('**/qbo.intuit.com/app/**', timeout=30000)
                                    break
                                except:
                                    logger.error("Code verification failed")
                                    save_screenshot(page.screenshot())
                                    raise Exception("SMS_VERIFICATION_FAILED")
                            
                            time.sleep(5)
                        else:
                            save_screenshot(page.screenshot())
                            raise Exception("SMS_VERIFICATION_TIMEOUT")
                    
                elif 'captcha' in page_text.lower() or 'robot' in page_text.lower():
                    raise Exception("CAPTCHA detected - wait and retry later")
                else:
                    raise Exception(f"Login stuck at {page.url}")
        
        # Go to banking
        human_delay(2, 3)
        print("Navigating to Banking page...", flush=True)
        page.goto('https://qbo.intuit.com/app/banking', timeout=30000)
        human_delay(3, 5)
        
        # Find and click the Update button
        print("Looking for Update button...", flush=True)
        update_btn = page.query_selector('button:has-text("Update")')
        
        if not update_btn:
            # Maybe it's already updating?
            updating_btn = page.query_selector('button:has-text("Updating")')
            if updating_btn:
                print("Already updating! Will wait for completion...", flush=True)
            else:
                print("Could not find Update button - continuing without refresh", flush=True)
                cookies = {c['name']: c['value'] for c in context.cookies() if 'intuit.com' in c.get('domain', '')}
                browser.close()
                return cookies
        else:
            print("Clicking Update button...", flush=True)
            update_btn.click()
            human_delay(2, 3)
        
        # Wait for "Updating" to appear then disappear
        print(f"Waiting for update to complete (timeout: {timeout}s)...", flush=True)
        logger.info(f"Waiting for bank feed update (timeout: {timeout}s)")
        
        start_time = time.time()
        
        # First wait for "Updating" to appear (confirms click worked)
        try:
            page.wait_for_selector('button:has-text("Updating")', timeout=10000)
            print("Update started (button shows 'Updating')", flush=True)
            logger.info("Bank feed update started")
        except:
            print("'Updating' button not found - may have already completed", flush=True)
        
        # Now wait for it to change back to "Update"
        while time.time() - start_time < timeout:
            elapsed = int(time.time() - start_time)
            
            # Check if we're back to "Update" button (not "Updating")
            try:
                # Look for button that has "Update" but not "Updating"
                btn = page.query_selector('button:has-text("Update")')
                if btn:
                    btn_text = btn.inner_text()
                    if 'Updating' not in btn_text and 'Update' in btn_text:
                        print(f"Bank feed update complete! ({elapsed}s)", flush=True)
                        logger.info(f"Bank feed update complete ({elapsed}s)")
                        break
            except:
                pass
            
            # Progress indicator every 30 seconds
            if elapsed % 30 == 0 and elapsed > 0:
                print(f"[{elapsed}s] Still updating...", flush=True)
            
            time.sleep(1)
        else:
            print(f"Timeout after {timeout}s - update may still be in progress", flush=True)
            logger.warning(f"Bank feed update timed out after {timeout}s")
        
        # Small delay then get cookies
        human_delay(2, 3)
        
        cookies = {c['name']: c['value'] for c in context.cookies() if 'intuit.com' in c.get('domain', '')}
        logger.info(f"Got session for company: {cookies.get('qbo.currentcompanyid')}")
        
        browser.close()
        return cookies


def scrape_quickbooks(cookies: Dict[str, str]):
    """Scrape accounts and transactions."""
    logger.info("Scraping QuickBooks...")
    
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
    
    # Accounts
    resp = requests.get(
        f'{QB_BASE_URL}/api/neo/v1/company/{company_id}/olb/ng/getInitialData',
        headers=headers, timeout=30
    )
    if resp.status_code != 200:
        raise Exception(f"Failed to get accounts: {resp.status_code}")
    
    accounts = resp.json().get('accounts', [])
    logger.info(f"Found {len(accounts)} accounts")
    
    # Transactions
    all_txns = []
    for acct in accounts:
        acct_id = str(acct.get('qboAccountId', ''))
        
        resp = requests.get(
            f'{QB_BASE_URL}/api/neo/v1/company/{company_id}/olb/ng/getTransactions',
            params={'accountId': acct_id, 'sort': '-txnDate', 'reviewState': 'PENDING', 'ignoreMatching': 'false'},
            headers={**headers, 'X-Range': 'items=0-499'},
            timeout=30
        )
        if resp.status_code != 200:
            continue
        
        for item in resp.json().get('items', []):
            amount = float(item.get('amount', 0))
            all_txns.append({
                'id': item.get('id', ''),
                'olb_txn_id': str(item.get('olbTxnId', '')),
                'date': item.get('olbTxnDate', '')[:10] if item.get('olbTxnDate') else '',
                'description': item.get('description', ''),
                'amount': abs(amount),
                'type': 'Expense' if amount < 0 else 'Income',
                'account_id': acct_id,
                'merchant_name': item.get('merchantName', ''),
            })
    
    logger.info(f"Found {len(all_txns)} transactions")
    return accounts, all_txns


def quickbase_request(method: str, endpoint: str, data: dict = None):
    """Make QuickBase API request."""
    print(f"QuickBase API: {method} {endpoint}", flush=True)
    resp = requests.request(
        method,
        f'https://api.quickbase.com/v1/{endpoint}',
        headers={
            'QB-Realm-Hostname': f'{QUICKBASE_REALM}.quickbase.com',
            'Authorization': f'QB-USER-TOKEN {QUICKBASE_TOKEN}',
            'Content-Type': 'application/json',
        },
        json=data,
        timeout=30
    )
    print(f"QuickBase response: {resp.status_code}", flush=True)
    if resp.status_code not in [200, 207]:
        print(f"QuickBase error: {resp.text[:500]}", flush=True)
    return resp


def sync_accounts(accounts: List) -> Dict[str, int]:
    """Sync accounts to QuickBase."""
    logger.info("Syncing accounts...")
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
    
    # Build mapping
    account_map = {}
    if resp.status_code == 200:
        for record in resp.json().get('data', []):
            qb_id = record.get(str(ACCOUNT_FIELDS['quickbooks_id']), {}).get('value')
            record_id = record.get('3', {}).get('value')
            if qb_id is not None and record_id:
                account_map[str(int(qb_id))] = record_id
    
    # Get full mapping
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
    
    logger.info(f"Mapped {len(account_map)} accounts")
    return account_map


def sync_bank_balances(accounts: List, account_map: Dict[str, int]) -> str:
    """
    Sync daily bank balance snapshots to QuickBase.
    
    Creates one record per account per day. Checks for existing records
    to prevent duplicates if run multiple times on the same day.
    """
    logger.info("Syncing bank balances (daily snapshot)...")
    
    if not BALANCES_TABLE_ID:
        logger.warning("BALANCES_TABLE_ID not set, skipping")
        return "skipped (no table)"
    
    today = date.today().isoformat()  # YYYY-MM-DD format
    
    records = []
    skipped = 0
    
    for acct in accounts:
        acct_id = str(acct.get('qboAccountId', ''))
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
    
    if skipped:
        logger.info(f"Skipped {skipped} accounts (no matching parent)")
    
    if not records:
        return "no records"
    
    # Check for existing balances for today
    logger.info(f"Checking for existing balances on {today}...")
    
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
        logger.info(f"Found {len(existing_accounts)} existing balance records for today")
        original_count = len(records)
        records = [
            r for r in records 
            if r[str(BALANCE_FIELDS['related_account'])]['value'] not in existing_accounts
        ]
        logger.info(f"Filtered to {len(records)} new balance records")
    
    if not records:
        return "already synced today"
    
    # Insert balance records
    logger.info(f"Inserting {len(records)} balance snapshot records...")
    
    resp = quickbase_request('POST', 'records', {
        'to': BALANCES_TABLE_ID,
        'data': records,
    })
    
    if resp.status_code == 200:
        meta = resp.json().get('metadata', {})
        created = len(meta.get('createdRecordIds', []))
        logger.info(f"Created {created} balance records")
        return f"{created} created"
    else:
        logger.error(f"Balance sync failed: {resp.status_code}")
        return f"error: {resp.status_code}"


def sync_transactions(transactions: List, account_map: Dict[str, int]):
    """Sync transactions to QuickBase."""
    logger.info("Syncing transactions...")
    
    records = []
    for t in transactions:
        parent_id = account_map.get(str(t['account_id']))
        if not parent_id:
            continue
        
        internal_id = t['id']
        if internal_id:
            numeric = ''.join(c for c in str(internal_id).split(':')[0] if c.isdigit())
            internal_id = int(numeric) if numeric else 0
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
            str(TRANSACTION_FIELDS['related_account']): {'value': parent_id},
        })
    
    if not records:
        logger.info("No transactions to sync")
        return
    
    # Batch upsert
    for i in range(0, len(records), 1000):
        batch = records[i:i + 1000]
        quickbase_request('POST', 'records', {
            'to': TRANSACTIONS_TABLE_ID,
            'data': batch,
            'mergeFieldId': TRANSACTION_FIELDS['quickbooks_id'],
        })
    
    logger.info(f"Synced {len(records)} transactions")


# =============================================================================
# GL Sync (OAuth-based)
# =============================================================================

def run_gl_sync() -> str:
    """
    Run GL sync using the OAuth-based integration.
    
    Requires OAuth tokens already set up and environment variables:
    - QB_CLIENT_ID, QB_CLIENT_SECRET
    - QUICKBASE_APP_ID
    """
    logger.info("Starting GL sync...")
    
    try:
        from qb_to_quickbase_sync import (
            load_config, TokenStore, QBOAuth, QuickBaseClient, SyncEngine
        )
    except ImportError:
        logger.warning("qb_to_quickbase_sync.py not found")
        return "skipped (module not found)"
    
    config = load_config()
    if not config:
        logger.warning("GL sync config not found")
        return "skipped (no config)"
    
    token_store = TokenStore()
    tokens = token_store.get_all()
    
    if not tokens:
        logger.warning("No OAuth tokens found")
        return "skipped (no tokens)"
    
    logger.info(f"Found {len(tokens)} connected companies")
    
    oauth = QBOAuth(config.client_id, config.client_secret, token_store)
    qb_client = QuickBaseClient(
        realm=config.quickbase_realm,
        token=config.quickbase_token,
        app_id=config.quickbase_app_id
    )
    
    engine = SyncEngine(oauth, qb_client)
    engine.sync_all()
    
    return f"synced {len(tokens)} companies"


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)