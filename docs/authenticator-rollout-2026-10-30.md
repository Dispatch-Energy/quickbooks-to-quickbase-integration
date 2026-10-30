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
