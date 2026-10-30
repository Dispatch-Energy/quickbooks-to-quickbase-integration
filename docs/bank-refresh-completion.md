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


## Azure acceptance — October 30, 2026

- Application code: `1ce6bd79b4790fb0bbddf8c9d3f50b9135afec30`; ACR build `ca6`.
- Active revision: `qb-sync--refresh-1ce6bd7`.
- Deployed digest: `sha256:dc416cd64c6d553720edb993e4447ed8dd95a9cb0057b5b30fc0f9d4587a58db`.
- Workflow run: `08584082089753256174574110757CU89`.
- Workflow start: `2026-10-30T22:05:10.1527467Z`; end: `2026-10-30T22:10:46.8316435Z`.
- The real authenticator challenge completed automatically at 22:05:52 UTC.
- Update was clicked at 22:06:08 UTC. Its UI cycle finished at 22:09:29 UTC,
  about 3 minutes 21 seconds later, with five connection errors reported.
- QuickBase writes began at 22:10:38 UTC and finished at 22:10:42 UTC.
- Logic Apps followed the job's 202 polling responses and received HTTP 200
  with `status=complete`, `outcome=partial`, and `held_accounts=5` at 22:10:46 UTC.
  The captured action result was the finished job, rather than `already_running`.

Independent read-only QuickBase queries confirmed:

| Check | Result |
| --- | --- |
| Account records | 124 total; 108 carry the new 22:10:38 UTC sync timestamp |
| Daily snapshots | 113 records for 113 distinct accounts; all prior record IDs preserved |
| Healthy snapshot values | All 108 match the refreshed Bank Account balance fields |
| Snapshot writes | 108 submitted by existing record ID; 3 changed, 0 created |
| Transactions | 10,036 processed; total remains 12,745, with no new rows |
| Held accounts | All five account and snapshot fingerprints unchanged |
| Held transactions | 551 records; identity/modification fingerprints unchanged |

The five remaining failures were source error 103 (invalid credentials). The
sixth feed identified earlier with a provider error recovered and became eligible.
Today's snapshots for the five held feeds existed before this run and retain
those earlier values; their date alone does not establish fresh bank data.

The service returned `sync_in_progress=false` afterward. Thirty-nine offline
regression tests passed. The separate GL step reported `skipped (module not found)`;
this acceptance establishes the bank-feed workflow and the requested holds.

The job store is process-local, as described above. Production uses one always-on
replica. Earlier test runs were explicitly cancelled before replacing revisions;
QuickBase readback established that those tests had not changed the destination.
