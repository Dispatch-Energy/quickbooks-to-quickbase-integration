#!/usr/bin/env python3
"""
QB Session Refresh Diagnostic

Tests whether API calls refresh session cookies and how long sessions last.
Run this to understand the token lifecycle before deploying.

Usage:
    python qb_session_diagnostic.py --test-refresh   # Check if API refreshes cookies
    python qb_session_diagnostic.py --cookie-expiry  # Show cookie expiration times
    python qb_session_diagnostic.py --monitor        # Monitor session over time
"""

import os
import sys
import json
import argparse
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from http.cookies import SimpleCookie

import requests

# Paths
DATA_DIR = Path.home() / '.qb_bank_scraper'
SESSION_DIR = DATA_DIR / 'session'
STATE_FILE = SESSION_DIR / 'state.json'

QB_API_KEY = 'prdakyresxaDrhFXaSARXaUdj1S8M7h6YK7YGekc'
QB_BASE_URL = 'https://qbo.intuit.com'


def load_session():
    """Load session and return parsed data"""
    with open(STATE_FILE, 'r') as f:
        return json.load(f)


def get_cookie_expiry(state: dict) -> dict:
    """Extract expiration info for key cookies"""
    results = {}
    key_cookies = ['qbo.ticket', 'qbo.tkt', 'qbo.csrftoken', 'ius_session', 'qbn.ticket']
    
    for cookie in state.get('cookies', []):
        name = cookie.get('name', '')
        if name in key_cookies or name.startswith('qbo.') or name.startswith('qbn.'):
            expires = cookie.get('expires', -1)
            if expires > 0:
                exp_dt = datetime.fromtimestamp(expires, timezone.utc)
                remaining = exp_dt - datetime.now(timezone.utc)
                results[name] = {
                    'expires': exp_dt.isoformat(),
                    'remaining_days': remaining.days,
                    'remaining_hours': remaining.total_seconds() / 3600
                }
            else:
                results[name] = {
                    'expires': 'session (no expiry set)',
                    'remaining_days': None,
                    'remaining_hours': None
                }
    
    return results


def test_api_refresh():
    """Make an API call and check if response includes Set-Cookie headers"""
    print("="*60)
    print("TESTING API COOKIE REFRESH")
    print("="*60)
    
    state = load_session()
    
    # Build cookies
    cookies = {}
    for cookie in state.get('cookies', []):
        if 'intuit.com' in cookie.get('domain', ''):
            cookies[cookie['name']] = cookie['value']
    
    company_id = cookies.get('qbo.currentcompanyid')
    
    # Headers
    headers = {
        'Accept': '*/*',
        'apiKey': QB_API_KEY,
        'Authorization': f'Intuit_APIKey intuit_apikey={QB_API_KEY}, intuit_apikey_version=1.0',
        'authType': 'browser_auth',
        'Content-Type': 'application/json',
        'intuit-company-id': company_id,
        'Referer': f'{QB_BASE_URL}/app/banking',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15',
    }
    
    if 'qbo.csrftoken' in cookies:
        headers['Csrftoken'] = cookies['qbo.csrftoken']
    
    # Make request with cookie jar to capture Set-Cookie
    session = requests.Session()
    
    # Set cookies
    for name, value in cookies.items():
        session.cookies.set(name, value, domain='.intuit.com')
    
    print(f"\n[1] Making API request to getInitialData...")
    response = session.get(
        f'{QB_BASE_URL}/api/neo/v1/company/{company_id}/olb/ng/getInitialData',
        headers=headers
    )
    
    print(f"    Status: {response.status_code}")
    
    # Check for Set-Cookie headers
    set_cookies = response.headers.get('Set-Cookie', '')
    
    print(f"\n[2] Checking Set-Cookie headers in response...")
    
    if set_cookies:
        print("    ✓ Response includes Set-Cookie headers!")
        print(f"    Raw: {set_cookies[:200]}...")
        
        # Parse and show refreshed cookies
        refreshed = []
        for cookie in session.cookies:
            if cookie.domain and 'intuit' in cookie.domain:
                refreshed.append(cookie.name)
        
        print(f"\n    Cookies potentially refreshed: {refreshed}")
    else:
        print("    ✗ No Set-Cookie headers in response")
        print("    Sessions may NOT be refreshed by API calls alone")
    
    # Check response cookies vs original
    print(f"\n[3] Comparing cookies before/after...")
    
    original_ticket = cookies.get('qbo.ticket', '')[:20]
    new_ticket = session.cookies.get('qbo.ticket', '')[:20] if session.cookies.get('qbo.ticket') else original_ticket
    
    if original_ticket != new_ticket:
        print(f"    ✓ qbo.ticket changed! Session is being refreshed.")
    else:
        print(f"    ○ qbo.ticket unchanged (may still be valid)")
    
    return response.status_code == 200


def show_cookie_expiry():
    """Show when key cookies expire"""
    print("="*60)
    print("COOKIE EXPIRATION TIMES")
    print("="*60)
    
    state = load_session()
    expiry = get_cookie_expiry(state)
    
    print(f"\nSession file: {STATE_FILE}")
    print(f"File modified: {datetime.fromtimestamp(STATE_FILE.stat().st_mtime).isoformat()}")
    print()
    
    # Sort by remaining time
    sorted_cookies = sorted(
        expiry.items(),
        key=lambda x: x[1].get('remaining_hours') or float('inf')
    )
    
    for name, info in sorted_cookies:
        remaining = info.get('remaining_hours')
        if remaining is not None:
            if remaining < 0:
                status = "⚠️  EXPIRED"
            elif remaining < 24:
                status = f"⚠️  {remaining:.1f} hours"
            elif remaining < 168:  # 1 week
                status = f"○  {info['remaining_days']} days"
            else:
                status = f"✓  {info['remaining_days']} days"
        else:
            status = "○  session cookie"
        
        print(f"  {name:30} {status}")
        if remaining is not None and remaining > 0:
            print(f"    {'':30} expires: {info['expires']}")
    
    print()
    
    # Find the limiting cookie
    min_expiry = None
    min_cookie = None
    for name, info in expiry.items():
        hours = info.get('remaining_hours')
        if hours is not None and hours > 0:
            if min_expiry is None or hours < min_expiry:
                min_expiry = hours
                min_cookie = name
    
    if min_expiry:
        print(f"⏰ Session limited by: {min_cookie}")
        print(f"   Estimated valid for: {min_expiry/24:.1f} days ({min_expiry:.0f} hours)")
    else:
        print("⏰ No expiring cookies found - session cookies only")
        print("   These typically last until browser close or server invalidation")


def monitor_session(interval_minutes=60, duration_hours=24):
    """Monitor session validity over time"""
    print("="*60)
    print(f"MONITORING SESSION (every {interval_minutes} min for {duration_hours} hours)")
    print("="*60)
    print("Press Ctrl+C to stop\n")
    
    state = load_session()
    cookies = {}
    for cookie in state.get('cookies', []):
        if 'intuit.com' in cookie.get('domain', ''):
            cookies[cookie['name']] = cookie['value']
    
    company_id = cookies.get('qbo.currentcompanyid')
    
    headers = {
        'Accept': '*/*',
        'apiKey': QB_API_KEY,
        'Authorization': f'Intuit_APIKey intuit_apikey={QB_API_KEY}, intuit_apikey_version=1.0',
        'authType': 'browser_auth',
        'intuit-company-id': company_id,
        'Referer': f'{QB_BASE_URL}/app/banking',
    }
    
    if 'qbo.csrftoken' in cookies:
        headers['Csrftoken'] = cookies['qbo.csrftoken']
    
    cookie_header = '; '.join(f'{k}={v}' for k, v in cookies.items())
    headers['Cookie'] = cookie_header
    
    start_time = datetime.now()
    check_count = 0
    
    try:
        while True:
            check_count += 1
            elapsed = datetime.now() - start_time
            
            response = requests.get(
                f'{QB_BASE_URL}/api/neo/v1/company/{company_id}/olb/ng/getInitialData',
                headers=headers,
                timeout=30
            )
            
            status = "✓" if response.status_code == 200 else "✗"
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"Check #{check_count} | "
                  f"Elapsed: {elapsed} | "
                  f"Status: {status} {response.status_code}")
            
            if response.status_code in [401, 403]:
                print(f"\n⚠️  SESSION EXPIRED after {elapsed}")
                print(f"   Total successful checks: {check_count - 1}")
                break
            
            if elapsed.total_seconds() > duration_hours * 3600:
                print(f"\n✓ Monitoring complete. Session still valid after {duration_hours} hours")
                break
            
            time.sleep(interval_minutes * 60)
            
    except KeyboardInterrupt:
        print(f"\n\nMonitoring stopped after {check_count} checks")
        print(f"Session was still valid at last check")


def main():
    parser = argparse.ArgumentParser(description='QB Session Diagnostic')
    parser.add_argument('--test-refresh', action='store_true', 
                       help='Test if API calls refresh cookies')
    parser.add_argument('--cookie-expiry', action='store_true',
                       help='Show cookie expiration times')
    parser.add_argument('--monitor', action='store_true',
                       help='Monitor session validity over time')
    parser.add_argument('--interval', type=int, default=60,
                       help='Monitor check interval in minutes (default: 60)')
    parser.add_argument('--duration', type=int, default=24,
                       help='Monitor duration in hours (default: 24)')
    
    args = parser.parse_args()
    
    if not STATE_FILE.exists():
        print(f"Error: No session file found at {STATE_FILE}")
        print("Run: python qb_http_scraper.py --login")
        sys.exit(1)
    
    if args.test_refresh:
        test_api_refresh()
    elif args.cookie_expiry:
        show_cookie_expiry()
    elif args.monitor:
        monitor_session(args.interval, args.duration)
    else:
        # Default: show all info
        show_cookie_expiry()
        print()
        test_api_refresh()


if __name__ == '__main__':
    main()
