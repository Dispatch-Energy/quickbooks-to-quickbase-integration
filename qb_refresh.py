"""Confirm the QuickBooks Update lifecycle before allowing downstream writes."""

import logging
import re
import time

logger = logging.getLogger(__name__)


class BankRefreshError(RuntimeError):
    pass


def select_healthy_accounts(accounts, refresh_report):
    """Hold explicit feed failures; reject ambiguous identity/status metadata."""
    healthy, held, seen = [], [], set()
    for account in accounts:
        identity = str(account.get('qboAccountId', ''))
        if (not identity.isdigit() or identity in seen
                or 'lastUpdateState' not in account or 'fdpErrorCode' not in account):
            raise BankRefreshError('BANK_REFRESH_ACCOUNT_STATUS_AMBIGUOUS')
        seen.add(identity)
        failed = (account['lastUpdateState'] not in ('Success', 'Unknown')
                  or account['fdpErrorCode'] not in (None, '', 0, '0')
                  or account.get('lastTekErrorCode') not in (None, '', 0, '0')
                  or account.get('lastTekErrorName') not in (None, '', 'NOTANERROR')
                  or account.get('reconnectOAuthAccount') in (True, 'true', 'True'))
        (held if failed else healthy).append(account)
    expected = refresh_report.get('failed_account_count')
    if refresh_report.get('connection_errors') and (not held or (expected and len(held) < expected)):
        raise BankRefreshError('BANK_REFRESH_UNMAPPED_CONNECTION_ERRORS')
    if not healthy:
        raise BankRefreshError('BANK_REFRESH_NO_HEALTHY_ACCOUNTS')
    return healthy, held


def _visible_button(page, pattern):
    buttons = [b for b in page.get_by_role("button", name=pattern).all()
               if b.is_visible()]
    if len(buttons) > 1:
        raise BankRefreshError("BANK_REFRESH_AMBIGUOUS_CONTROL")
    return buttons[0] if buttons else None


def wait_for_bank_refresh(page, timeout=600, *, clock=time.monotonic, sleep=time.sleep):
    """Click Update once, then require a witnessed update and an idle control.

    A missing control, an unobserved start, HTTP error, or timeout fails the sync.
    Completed connection errors are returned for exact downstream account holds.
    """
    # Current QBO labels the button "Updating accounts" / "Update accounts"
    # for accessibility, while its visible child span just says "Updating".
    update = re.compile(r"^Update(?: accounts)?$", re.I)
    updating = re.compile(r"^Updating(?: accounts)?(?:\.{3}|…)?$", re.I)
    deadline = clock() + timeout
    control_deadline = min(deadline, clock() + 30)
    observed = {"started": False, "api_seen": False, "complete": False,
                "failed": False, "http_error": False}

    def response_received(response):
        # Observe the browser's own polling; do not POST another Update job.
        if "/olb/manualUpdate/" not in response.url:
            return
        if response.status >= 400:
            observed["http_error"] = True
            return
        try:
            body = response.json()
        except Exception:
            return
        if not isinstance(body, dict):
            return
        jobs = body.get("subJobs", [])
        if body.get("hasErrors") or any(j.get("hasError") for j in jobs):
            observed["failed"] = True
        if jobs:
            observed["api_seen"] = True
            observed["started"] = True
            observed["complete"] = body.get("isComplete") is True
            logger.info("Bank Update response: complete=%s, banks=%s, failed_banks=%s",
                        observed['complete'], len(jobs), sum(bool(j.get('hasError')) for j in jobs))

    page.on("response", response_received)
    try:
        while True:
            busy = _visible_button(page, updating)
            button = _visible_button(page, update)
            if busy:
                observed["started"] = True
                logger.info("Bank feed Update is already running; waiting")
                break
            if button and button.is_enabled():
                logger.info("Clicking bank feed Update")
                button.click()
                break
            if clock() >= control_deadline:
                raise BankRefreshError("BANK_REFRESH_UPDATE_CONTROL_MISSING")
            page.wait_for_timeout(250)
            sleep(0.25)

        start_deadline = min(deadline, clock() + 30)
        idle_since = None
        next_progress = clock()
        while clock() < deadline:
            # Playwright pumps browser events here, including response callbacks.
            page.wait_for_timeout(250)
            if observed["http_error"]:
                raise BankRefreshError("BANK_REFRESH_API_ERROR")
            busy = _visible_button(page, updating)
            button = _visible_button(page, update)
            if busy:
                observed["started"] = True
                idle_since = None
            elif (observed["started"] and button and button.is_enabled()
                  and (not observed["api_seen"] or observed["complete"])):
                if idle_since is None:
                    idle_since = clock()
                # Avoid accepting a transient button rerender as completion.
                if clock() - idle_since >= 2:
                    warning = re.search(r"Unable to get transactions for (\d+) accounts?",
                                        page.inner_text('body'), re.I)
                    if observed["failed"] or warning:
                        count = int(warning.group(1)) if warning else None
                        logger.warning("Bank Update finished with connection errors; accounts=%s", count)
                        return {'connection_errors': True, 'failed_account_count': count}
                    logger.info("Bank feed Update completion confirmed")
                    return {'connection_errors': False, 'failed_account_count': 0}
            else:
                idle_since = None
            if not observed["started"] and clock() >= start_deadline:
                raise BankRefreshError("BANK_REFRESH_START_NOT_CONFIRMED")
            if clock() >= next_progress:
                logger.info("Waiting for bank feed Update completion")
                next_progress = clock() + 30
            sleep(0.75)
        raise BankRefreshError("BANK_REFRESH_TIMEOUT")
    finally:
        page.remove_listener("response", response_received)
