"""Offline refresh lifecycle, concurrency, and write-gating regressions."""

import json
import threading
import time
import unittest
from unittest.mock import Mock, patch

from fastapi import HTTPException
from starlette.requests import Request

import app
from qb_refresh import BankRefreshError, wait_for_bank_refresh


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
                    'isComplete': self.mode == 'api_complete',
                    'hasErrors': self.mode == 'bank_error'}
            self.listener(Mock(url='https://qbo.intuit.com/api/neo/v2/company/test/olb/manualUpdate/start',
                               status=200, json=lambda: body))

    def get_by_role(self, role, name):
        if self.mode == 'missing':
            text = None
        elif self.mode == 'already_running':
            text = 'Updating' if self.clock() < 4 else 'Update'
        elif self.clicked and self.mode in ('success', 'timeout'):
            text = 'Updating' if self.clock() < 4 or self.mode == 'timeout' else 'Update'
        else:
            text = 'Update'
        button = Mock(is_visible=lambda: True, is_enabled=lambda: True)
        button.click = lambda: setattr(self, 'clicked', self.clicked + 1)
        return Mock(all=lambda: [button] if text and name.search(text) else [])


class RefreshTests(unittest.TestCase):
    def refresh(self, mode):
        clock = Clock()
        page = Page(clock, mode)
        wait_for_bank_refresh(page, timeout=40, clock=clock, sleep=clock.sleep)
        self.assertIsNone(page.listener)
        return page

    def test_clicks_once_then_waits_for_completion(self):
        self.assertEqual(self.refresh('success').clicked, 1)

    def test_waits_for_update_already_in_progress_without_clicking(self):
        self.assertEqual(self.refresh('already_running').clicked, 0)

    def test_fast_api_confirmed_completion(self):
        self.assertEqual(self.refresh('api_complete').clicked, 1)

    def test_missing_unconfirmed_pending_error_and_timeout_fail(self):
        for mode, reason in [('missing', 'CONTROL_MISSING'), ('idle', 'START_NOT_CONFIRMED'),
                             ('timeout', 'TIMEOUT'), ('bank_error', 'BANK_ERROR'),
                             ('api_pending', 'TIMEOUT')]:
            with self.subTest(mode=mode), self.assertRaisesRegex(BankRefreshError, reason):
                self.refresh(mode)


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
