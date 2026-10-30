# Wait for bank-feed Update before syncing

The daily `qb-sync-scheduler` HTTP action must use
[`ops/refresh-sync-action.json`](../ops/refresh-sync-action.json). Its 1 PM Mountain
recurrence is unchanged. That action starts `/sync-jobs` with `refresh_feeds=true`
and allows up to 20 minutes for the QuickBooks Update step. The overall action
allows 30 minutes including login, scraping, and QuickBase writes.

The API returns 202 with `Location` and `Retry-After: 15` while the specific job
runs. Logic Apps follows that location until the operation returns 200 on success
or an error on failure. The workflow run ID is its idempotency key: repeating a
start request follows the same job. A different concurrent run returns 409, never
a successful `already_running` response. Automatic HTTP retries are disabled.

The refresh clicks Update once, or joins an already-visible Updating cycle. It
requires observed progress followed by an enabled Update button stable for two
seconds. An API start response can confirm that work was accepted, but its pending
snapshot does not override a later completed UI cycle. Missing controls, an unconfirmed start, HTTP errors,
or timeout stop before bank-data scraping and QuickBase writes. Once Update
finishes, failed feeds are held using their source account IDs and connection
status. Healthy feeds continue; held feeds receive no account, snapshot, or
transaction writes. Ambiguous status metadata or errors that cannot be mapped
to held accounts stop the whole run. A completed job reports `outcome=partial`
and the held count when feeds are excluded, following the owner's policy.
Completing this process does not guarantee that every bank supplies real-time
data; institution availability and QuickBooks' source timestamps still matter.

After a confirmed refresh, today's balance snapshots update by their existing
record IDs; absent account/date pairs are inserted. Ordinary cached-data copies
retain the previous insert-only behavior. Zero bank balances stay zero. Ambiguous
duplicate snapshots, a failed/truncated lookup, and failed/partial QuickBase
writes are errors rather than successful completion.

## Hosting and recovery

This app runs one Uvicorn process and exactly one replica. A minimum of one
replica keeps its background worker alive while no HTTP request is active.
Both the deployment workflow and the live Container App must retain minimum 1,
maximum 1. This uses more idle compute than the previous minimum of zero.

Job history and idempotency are process-local, limited to the latest 100 jobs.
A restart loses that history: the status endpoint returns 404 and the workflow
fails instead of claiming completion or starting another operation. After a
restart/failure, inspect logs and QuickBase records before retrying. Do not deploy
or change replica settings during a running sync. Durable cross-restart jobs or
multiple replicas require a shared queue/job store and distributed lock.

The legacy `/sync` and `/sync-all` endpoints remain synchronous and retain their
cached-data default. Scheduled long-running refreshes must use `/sync-jobs`.
The separate GL script remains outside the current container image; its skipped
result is reported independently from bank-feed completion.

Reference: [Microsoft Logic Apps asynchronous polling pattern](https://learn.microsoft.com/en-us/azure/logic-apps/logic-apps-create-api-app#perform-long-running-tasks-with-the-polling-action-pattern).

## Offline validation

Run `python -m unittest test_qb_auth test_refresh_sync -q`. Tests cover the
authenticator path, Update completion/error/timeout, no writes after failed
refresh, repeated/concurrent job requests, failed/lost jobs, and daily snapshot
updates without duplicates or zero-balance substitution.
