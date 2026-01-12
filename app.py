"""
QuickBooks → QuickBase Sync - Container App (v2)

FastAPI app with endpoints:
- POST /sync - trigger bank feeds sync (accounts, balances, transactions)
- POST /sync-gl - trigger GL sync (OAuth-based)
- POST /sync-all - trigger both bank feeds + GL sync
- POST /code - submit SMS verification code
- GET /screenshot - view latest screenshot
- GET /health - health check
"""

import os
import time
import random
import logging
import threading
from datetime import datetime, timezone, date
from typing import Dict, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import Response
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


@app.get("/health")
def health_check():
    return {
        "status": "ok", 
        "sync_in_progress": state.sync_in_progress,
        "version": "2.0",
        "features": ["bank_feeds", "bank_balances", "gl_sync"]
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
    """Trigger bank feeds sync. If verification needed, waits up to 3 min for /code."""
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
        
        result = run_bank_feeds_sync(
            skip_balances=skip_balances,
            skip_transactions=skip_transactions
        )
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
    """Trigger both bank feeds and GL sync."""
    if state.sync_in_progress:
        return {"status": "already_running", "message": "Sync already in progress"}
    
    try:
        state.sync_in_progress = True
        state.pending_sms_code = None
        
        skip_balances = req.skip_balances if req else False
        skip_transactions = req.skip_transactions if req else False
        
        # Run bank feeds first
        bank_result = run_bank_feeds_sync(
            skip_balances=skip_balances,
            skip_transactions=skip_transactions
        )
        
        # Then run GL sync
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
                                    page.wait_for_url('**/qbo.intuit.com/app/**', timeout=30000)
                                    logger.info("Verification successful!")
                                    break
                                except:
                                    save_screenshot(page.screenshot())
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