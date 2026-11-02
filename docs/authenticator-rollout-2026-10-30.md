# Intuit authenticator rollout — October 30, 2026

## Deployment

- Container App: `qb-sync`, resource group `cabbagepatchkids`.
- Application code commit: `ea6dced964906dea81b81e20bb455b2a513c97aa`.
- ACR build: `ca2`, image tag `qb-sync:auth-ea6dced`.
- Deployed digest: `sha256:23fa9e352dd1b8a0be628abdb288c3148517b52a4fe8d3c921db92f6b58705bb`.
- Active revision: `qb-sync--auth-ea6dced`.
- `QB_TOTP_SECRET` references the Container Apps secret `qb-totp-secret`, backed
  by a versioned Key Vault reference. The system-assigned identity has read
  access scoped to that single vault secret.
- The new enrollment key was read from the operator's ignored `.env`, validated,
  and independently compared with Key Vault readback. No key or generated code
  was printed or committed. The build context contained only application inputs.

## Acceptance evidence

Intuit's account settings showed the authenticator enabled after the owner
completed re-enrollment. An authentication-only process inside the new Azure
revision then ran the real `app.auto_login()` path with the configured key:

```json
{
  "authentication_only": true,
  "quickbase_writes": false,
  "totp_challenge_seen": true,
  "totp_code_generated": true,
  "totp_verified": true,
  "authentication_ok": true,
  "company_context_present": true
}
```

The test reached an actual Intuit verification-code challenge, generated a TOTP,
completed sign-in, and obtained the company context after navigating to banking.
No phone code was supplied and no QuickBase sync was invoked. The service's
`/health` endpoint returned HTTP 200 afterward with `sync_in_progress=false`.

All 20 offline tests passed, including dispatch through both browser login entry
points. Live acceptance exercised `auto_login()`; it did not run the bank-feed
Update action, write balance snapshots, or validate every bank connection.

Future re-enrollment requires replacing the vault secret and its versioned
reference together. Unsupported or additional identity challenges still stop
the configured authenticator flow rather than guessing codes or selecting SMS.

## Observed Azure workflow run

At the owner's request, the existing `qb-sync-scheduler` workflow was manually
triggered at 21:03 UTC (3:03 PM Mountain). Run ID:
`08584082127000090059509448732CU91`. The daily scheduled invocation had already
run at 19:00 UTC, before this authenticator rollout.

The serving revision observed during this run was `qb-sync--0000015`, with image
tag `c7f373e5b5a9564b0fcce9874b7114d5aad79130`, superseding the initial deployment
above. Its logs recorded successful authenticator verification at 21:03:49 UTC,
113 scraped accounts, and 10,562 scraped transactions. Bank sync writes completed
at approximately 21:05:17 UTC. Independent read-only QuickBase queries confirmed:

| Check | Before | After |
| --- | --- | --- |
| Account records | 124 | 124 |
| Latest account sync timestamp | October 29, 19:03:30 UTC | October 30, 21:05:12 UTC |
| Accounts bearing the new sync timestamp | — | 113 |
| October 30 balance snapshots | 0 | 113, for 113 distinct accounts |
| Transaction records | 12,675 | 12,745 |
| Latest transaction modification | October 29, 19:03:35 UTC | October 30, 21:05:16 UTC |

The service subsequently returned `sync_in_progress=false`. No phone code was
supplied. The workflow's existing empty request body did not request the bank-feed
Update action; this run copied the data already available in QuickBooks. The GL
step was skipped because `qb_to_quickbase_sync.py` was absent from the container.

The Logic App reported success at 21:05:11 UTC, but its captured response was
`already_running`, consistent with an HTTP retry while the original request was
still executing. Scheduler success alone therefore does not prove business
completion. Logs and durable QuickBase readback establish this run's bank-sync
completion. No scheduler configuration was changed during observation.
