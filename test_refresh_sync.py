"""Offline refresh lifecycle, concurrency, and write-gating regressions."""

import json
import threading
import time
import unittest
from unittest.mock import Mock, patch

from fastapi import HTTPException
from starlette.requests import Request

import app
from qb_refresh import BankRefreshError, wait_for_bank_refresh, select_healthy_accounts


class Clock:
    now = 0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Page:
    def __init__(self, clock, mode):
        self.clock, self.mode = clock, mode
        self.clicked, self.listener = 0, None

    def on(self, event, listener):
        self.listener = listener

    def remove_listener(self, event, listener):
        self.listener = None

    def wait_for_timeout(self, milliseconds):
        self.clock.sleep(milliseconds / 1000)
        if self.mode in ('bank_error', 'api_pending', 'api_complete') and self.clicked:
            body = {'subJobs': [{'hasError': self.mode == 'bank_error'}],
                    'isComplete': self.mode in ('api_complete', 'bank_error'),
                    'hasErrors': self.mode == 'bank_error'}
            self.listener(Mock(url='https://qbo.intuit.com/api/neo/v2/company/test/olb/manualUpdate/start',
                               status=200, json=lambda: body))

    def inner_text(self, selector):
        return 'Unable to get transactions for 5 accounts' if self.mode == 'ui_error' else ''

    def get_by_role(self, role, name):
        if self.mode == 'missing':
            text = None
        elif self.mode == 'already_running':
            text = 'Updating' if self.clock() < 4 else 'Update'
        elif self.clicked and self.mode in ('success', 'timeout', 'accessible_label', 'ui_error'):
            text = 'Updating' if self.clock() < 4 or self.mode == 'timeout' else 'Update'
        else:
            text = 'Update'
        if self.mode == 'accessible_label':
            text += ' accounts'
        button = Mock(is_visible=lambda: True, is_enabled=lambda: True)
        button.click = lambda: setattr(self, 'clicked', self.clicked + 1)
        return Mock(all=lambda: [button] if text and name.search(text) else [])


class RefreshTests(unittest.TestCase):
    def refresh(self, mode):
        clock = Clock()
        page = Page(clock, mode)
        page.report = wait_for_bank_refresh(page, timeout=40, clock=clock, sleep=clock.sleep)
        self.assertIsNone(page.listener)
        return page

    def test_clicks_once_then_waits_for_completion(self):
        self.assertEqual(self.refresh('success').clicked, 1)

    def test_waits_for_update_already_in_progress_without_clicking(self):
        self.assertEqual(self.refresh('already_running').clicked, 0)

    def test_fast_api_confirmed_completion(self):
        self.assertEqual(self.refresh('api_complete').clicked, 1)

    def test_current_qbo_accessibility_labels(self):
        self.assertEqual(self.refresh('accessible_label').clicked, 1)

    def test_missing_unconfirmed_pending_error_and_timeout_fail(self):
        for mode, reason in [('missing', 'CONTROL_MISSING'), ('idle', 'START_NOT_CONFIRMED'),
                             ('timeout', 'TIMEOUT'), ('api_pending', 'TIMEOUT')]:
            with self.subTest(mode=mode), self.assertRaisesRegex(BankRefreshError, reason):
                self.refresh(mode)

    def test_completed_connection_errors_are_reported_for_exact_account_holds(self):
        for mode in ('bank_error', 'ui_error'):
            with self.subTest(mode=mode):
                page = self.refresh(mode)
                self.assertTrue(page.report['connection_errors'])
                self.assertGreaterEqual(page.clock(), 2)


def feed(identity, state='Success', code=0, **extra):
    return dict(qboAccountId=identity, lastUpdateState=state, fdpErrorCode=code, **extra)


class EligibilityTests(unittest.TestCase):
    def test_healthy_oauth_unknown_status_and_failed_feeds_are_separated(self):
        good = [feed('1'), feed('2', 'Unknown', reconnectOAuthAccount=False)]
        bad = [feed('3', 'Failure', 103), feed('4', 'Failure', 3000),
               feed('5', lastTekErrorCode=103), feed('6', reconnectOAuthAccount=True)]
        healthy, held = select_healthy_accounts(good + bad, {'connection_errors': True, 'failed_account_count': 2})
        self.assertEqual(healthy, good)
        self.assertEqual(held, bad)

    def test_missing_duplicate_unmapped_or_all_failed_accounts_stop_sync(self):
        cases = [([{'qboAccountId': '1'}], {}), ([feed('1'), feed('1')], {}),
                 ([feed('1')], {'connection_errors': True}),
                 ([feed('1'), feed('2', 'Failure', 103)],
                  {'connection_errors': True, 'failed_account_count': 2}),
                 ([feed('1', 'Failure', 103)], {})]
        for accounts, report in cases:
            with self.subTest(accounts=accounts), self.assertRaises(BankRefreshError):
                select_healthy_accounts(accounts, report)


def response(data=None, metadata=None, status=200):
    return Mock(status_code=status, json=lambda: {'data': data or [], 'metadata': metadata or {}})


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.state = patch.object(app, 'state', app.AppState())
        self.state.start()
        self.addCleanup(self.state.stop)
        config = patch.multiple(app, QB_USERNAME='test', QB_PASSWORD='test', QUICKBASE_TOKEN='test',
                                ACCOUNTS_TABLE_ID='accounts', TRANSACTIONS_TABLE_ID='transactions',
                                BALANCES_TABLE_ID='balances')
        config.start()
        self.addCleanup(config.stop)

    def request(self, key='test-run'):
        return Request({'type': 'http', 'scheme': 'https', 'server': ('testserver', 443),
                        'path': '/sync-jobs', 'headers': [(b'idempotency-key', key.encode())],
                        'router': app.app.router})

    def wait_finished(self):
        for _ in range(200):
            if not app.state.sync_in_progress:
                return
            time.sleep(.005)
        self.fail('Worker failed to finish')

    def test_retries_follow_same_job_and_do_not_report_busy_as_success(self):
        entered, release = threading.Event(), threading.Event()
        def execute(req):
            entered.set()
            release.wait(2)
            return {'status': 'complete', 'result': 'verified test result'}
        with patch.object(app, 'execute_sync', side_effect=execute) as run:
            first = app.start_sync_job(self.request(), app.SyncJobRequest())
            self.assertTrue(entered.wait(1))
            try:
                self.assertEqual(first.status_code, 202)
                self.assertTrue(first.headers['location'].endswith('/sync-jobs/test-run'))
                self.assertEqual(app.start_sync_job(self.request(), app.SyncJobRequest()).status_code, 202)
                self.assertEqual(app.sync_job_status('test-run', self.request()).status_code, 202)
                with self.assertRaises(HTTPException) as busy:
                    app.start_sync_job(self.request('different-run'), app.SyncJobRequest())
                self.assertEqual(busy.exception.status_code, 409)
                with self.assertRaises(HTTPException) as legacy_busy:
                    app.trigger_sync()
                self.assertEqual(legacy_busy.exception.status_code, 409)
                with self.assertRaises(HTTPException):
                    app.start_sync_job(self.request(), app.SyncJobRequest(skip_transactions=True))
            finally:
                release.set()
                self.wait_finished()
            final = app.sync_job_status('test-run', self.request())
            self.assertEqual(final.status_code, 200)
            self.assertEqual(json.loads(final.body)['result'], 'verified test result')
            app.start_sync_job(self.request(), app.SyncJobRequest())
            self.assertEqual(run.call_count, 1)

    def test_failed_or_lost_job_is_never_success(self):
        with patch.object(app, 'execute_sync', side_effect=RuntimeError('test failure')):
            app.start_sync_job(self.request())
            self.wait_finished()
        result = app.sync_job_status('test-run', self.request())
        self.assertEqual(result.status_code, 500)
        self.assertEqual(json.loads(result.body)['status'], 'failed')
        self.assertTrue(app.state.sync_lock.acquire(blocking=False))
        app.state.sync_lock.release()
        with self.assertRaises(HTTPException) as missing:
            app.sync_job_status('missing', self.request())
        self.assertEqual(missing.exception.status_code, 404)

    def test_refresh_failure_prevents_scraping_and_all_quickbase_writes(self):
        with patch.object(app, 'auto_login_and_refresh', side_effect=BankRefreshError('BANK_REFRESH_TIMEOUT')), \
             patch.object(app, 'run_gl_sync', return_value='skipped'), \
             patch.object(app, 'scrape_quickbooks') as scrape, \
             patch.object(app, 'quickbase_request') as qb:
            with self.assertRaises(HTTPException):
                app.trigger_sync(app.SyncRequest(refresh_feeds=True))
            scrape.assert_not_called()
            qb.assert_not_called()
            self.assertFalse(app.state.sync_in_progress)

    def test_failed_feeds_never_reach_account_snapshot_or_transaction_writes(self):
        good, bad = feed('1'), feed('2', 'Failure', 103)
        good_txn, bad_txn = {'account_id': '1'}, {'account_id': '2'}
        session = app.RefreshedSession({'qbo.currentcompanyid': 'test'},
                                       {'connection_errors': True, 'failed_account_count': 1})
        with patch.object(app, 'auto_login_and_refresh', return_value=session), \
             patch.object(app, 'run_gl_sync', return_value='skipped'), \
             patch.object(app, 'scrape_quickbooks', return_value=([good, bad], [good_txn, bad_txn])), \
             patch.object(app, 'sync_accounts', return_value={'1': 9, '2': 10}) as accounts, \
             patch.object(app, 'sync_bank_balances', return_value='0 created, 1 updated') as balances, \
             patch.object(app, 'sync_transactions') as transactions:
            result = app.trigger_sync(app.SyncJobRequest())
        accounts.assert_called_once_with([good])
        self.assertEqual(balances.call_args.args[0], [good])
        self.assertTrue(balances.call_args.kwargs['replace_existing'])
        self.assertEqual(transactions.call_args.args[0], [good_txn])
        self.assertEqual(result['held_accounts'], 1)
        self.assertEqual(result['outcome'], 'partial')

    def test_scraper_does_not_request_failed_feed_transactions(self):
        good, bad = feed('1'), feed('2', 'Failure', 103)
        initial = Mock(status_code=200, json=lambda: {'accounts': [good, bad]})
        empty = Mock(status_code=200, json=lambda: {'items': []})
        with patch.object(app.requests, 'get', side_effect=[initial, empty]) as get:
            accounts, transactions = app.scrape_quickbooks({'qbo.currentcompanyid': 'test'})
        self.assertEqual(accounts, [good, bad])
        self.assertEqual(transactions, [])
        self.assertEqual(get.call_count, 2)
        self.assertEqual(get.call_args.kwargs['params']['accountId'], '1')

    def test_failed_healthy_feed_transaction_request_is_not_silently_skipped(self):
        with patch.object(app.requests, 'get', side_effect=[
                Mock(status_code=200, json=lambda: {'accounts': [feed('1')]}),
                Mock(status_code=503)]):
            with self.assertRaisesRegex(RuntimeError, 'TRANSACTIONS_FAILED_HTTP_503'):
                app.scrape_quickbooks({'qbo.currentcompanyid': 'test'})

    def test_confirmed_refresh_updates_existing_daily_record_and_keeps_zero(self):
        existing = [{'3': {'value': 42}, '8': {'value': 9}}]
        with patch.object(app, 'quickbase_request', side_effect=[response(existing), response(metadata={
                'updatedRecordIds': [42]})]) as qb:
            result = app.sync_bank_balances([{'qboAccountId': 1, 'bankBalance': 0, 'qboBalance': 99}],
                                           {'1': 9}, replace_existing=True)
        record = qb.call_args_list[-1].args[2]['data'][0]
        self.assertEqual(record['3']['value'], 42)
        self.assertEqual(record['6']['value'], 0)
        self.assertEqual(result, '0 created, 1 updated')

    def test_ordinary_copy_preserves_existing_daily_snapshot(self):
        with patch.object(app, 'quickbase_request', return_value=response([
                {'3': {'value': 42}, '8': {'value': 9}}])) as qb:
            result = app.sync_bank_balances([{'qboAccountId': 1, 'bankBalance': 5}], {'1': 9})
        self.assertEqual(result, 'already synced today')
        self.assertEqual(qb.call_count, 1)

    def test_failed_duplicate_or_truncated_balance_query_prevents_writes(self):
        for query in [response(status=500), response([{'3': {'value': 42}, '8': {'value': 9}},
                                                     {'3': {'value': 43}, '8': {'value': 9}}]),
                      response(metadata={'totalRecords': 2000})]:
            with self.subTest(query=query), patch.object(app, 'quickbase_request', return_value=query) as qb:
                with self.assertRaises(RuntimeError):
                    app.sync_bank_balances([{'qboAccountId': 1, 'bankBalance': 5}], {'1': 9}, True)
                self.assertEqual(qb.call_count, 1)

    def test_partial_quickbase_write_is_a_failure(self):
        for result in [response(status=207), response(metadata={'lineErrors': {'1': ['rejected']}})]:
            with self.subTest(result=result), self.assertRaises(RuntimeError):
                app.require_quickbase_success(result, 'TEST')


if __name__ == '__main__':
    unittest.main()
