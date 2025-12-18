#!/usr/bin/env python3
"""
QB Scraper Runner - Main Entry Point for Container

Flow:
1. Auto-login via Playwright (headless)
2. Scrape pending transactions via HTTP API
3. Sync to QuickBase
4. Send alerts on failure

Environment Variables Required:
    QB_USERNAME         - QuickBooks login email
    QB_PASSWORD         - QuickBooks password
    QUICKBASE_REALM     - QuickBase realm
    QUICKBASE_TOKEN     - QuickBase API token
    QUICKBASE_TABLE_ID  - Target table ID
    ALERT_WEBHOOK_URL   - (optional) Teams/Slack webhook
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from dataclasses import asdict

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('qb_scraper')

# Environment
QB_USERNAME = os.getenv('QB_USERNAME')
QB_PASSWORD = os.getenv('QB_PASSWORD')
QUICKBASE_REALM = os.getenv('QUICKBASE_REALM')
QUICKBASE_TOKEN = os.getenv('QUICKBASE_TOKEN')
QUICKBASE_TABLE_ID = os.getenv('QUICKBASE_TABLE_ID')
ALERT_WEBHOOK_URL = os.getenv('ALERT_WEBHOOK_URL')


def send_alert(title: str, message: str, severity: str = 'warning'):
    """Send alert via Teams/Slack webhook"""
    if not ALERT_WEBHOOK_URL:
        logger.warning(f"Alert (no webhook): {title} - {message}")
        return
    
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "FF0000" if severity == 'error' else "00FF00" if severity == 'success' else "FFA500",
        "summary": title,
        "sections": [{
            "activityTitle": f"🔔 QB Scraper: {title}",
            "facts": [
                {"name": "Severity", "value": severity.upper()},
                {"name": "Time", "value": datetime.now(timezone.utc).isoformat()},
                {"name": "Message", "value": message}
            ]
        }]
    }
    
    try:
        requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")


def run():
    """Main execution flow"""
    start_time = datetime.now(timezone.utc)
    logger.info(f"Starting QB scraper at {start_time.isoformat()}")
    
    # Validate environment
    if not QB_USERNAME or not QB_PASSWORD:
        error = "QB_USERNAME and QB_PASSWORD environment variables required"
        logger.error(error)
        send_alert("Configuration Error", error, severity='error')
        return False
    
    # Step 1: Auto-login via Playwright
    logger.info("="*60)
    logger.info("STEP 1: Auto-login via Playwright")
    logger.info("="*60)
    
    try:
        from qb_auto_login import QBAutoLogin
        
        with QBAutoLogin(headless=True) as qb:
            if not qb.ensure_logged_in(QB_USERNAME, QB_PASSWORD):
                error = "Auto-login failed"
                logger.error(error)
                send_alert("Login Failed", error, severity='error')
                return False
        
        logger.info("✓ Login successful, session saved")
        
    except Exception as e:
        error = f"Login error: {e}"
        logger.exception(error)
        send_alert("Login Error", error, severity='error')
        return False
    
    # Step 2: Scrape via HTTP API
    logger.info("="*60)
    logger.info("STEP 2: Scrape transactions via HTTP API")
    logger.info("="*60)
    
    try:
        from qb_http_scraper import QBBankScraper
        
        scraper = QBBankScraper()
        
        # Verify session
        if not scraper.check_session():
            error = "Session invalid after login"
            logger.error(error)
            send_alert("Session Error", error, severity='error')
            return False
        
        # Scrape all accounts
        accounts, transactions = scraper.scrape_all_pending()
        
        logger.info(f"✓ Scraped {len(transactions)} transactions from {len(accounts)} accounts")
        
        # Log breakdown by account
        by_account = {}
        for t in transactions:
            key = t.account_name or t.account_id
            by_account[key] = by_account.get(key, 0) + 1
        
        for acct, count in sorted(by_account.items()):
            logger.info(f"    {acct}: {count} pending")
        
    except Exception as e:
        error = f"Scrape error: {e}"
        logger.exception(error)
        send_alert("Scrape Error", error, severity='error')
        return False
    
    # Step 3: Sync to QuickBase
    logger.info("="*60)
    logger.info("STEP 3: Sync to QuickBase")
    logger.info("="*60)
    
    if not all([QUICKBASE_REALM, QUICKBASE_TOKEN, QUICKBASE_TABLE_ID]):
        logger.warning("QuickBase not configured - skipping sync")
        sync_result = {'skipped': True}
    else:
        try:
            from qb_http_scraper import QuickBaseSync
            
            sync = QuickBaseSync(QUICKBASE_REALM, QUICKBASE_TOKEN, QUICKBASE_TABLE_ID)
            sync_result = sync.sync_transactions(transactions)
            
            if sync_result.get('success'):
                logger.info(f"✓ QuickBase sync: {sync_result.get('created', 0)} created, "
                           f"{sync_result.get('updated', 0)} updated")
            else:
                error = f"QuickBase sync failed: {sync_result.get('error')}"
                logger.error(error)
                send_alert("Sync Error", error, severity='error')
                # Don't return False - scrape succeeded
                
        except Exception as e:
            error = f"Sync error: {e}"
            logger.exception(error)
            send_alert("Sync Error", error, severity='error')
            sync_result = {'error': str(e)}
    
    # Summary
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    
    logger.info("="*60)
    logger.info("COMPLETE")
    logger.info("="*60)
    logger.info(f"Duration: {duration:.1f}s")
    logger.info(f"Accounts: {len(accounts)}")
    logger.info(f"Transactions: {len(transactions)}")
    logger.info(f"Sync: {sync_result}")
    
    # Success alert (optional - can be noisy)
    # send_alert(
    #     "Scrape Complete",
    #     f"Scraped {len(transactions)} transactions from {len(accounts)} accounts in {duration:.1f}s",
    #     severity='success'
    # )
    
    return True


if __name__ == '__main__':
    success = run()
    sys.exit(0 if success else 1)
