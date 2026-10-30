# QuickBooks bank-feed authenticator setup

This applies to the Container App (`app.py`), in both normal bank scraping and
`refresh_feeds=true` login paths. It does not change the separate accounting
OAuth integration, bank-provider authentication, or QuickBase field mappings.

## Get the enrollment secret

The secret is the long **manual setup key next to the QR code** in Intuit's
authenticator enrollment screen. It is not the rotating six-digit code.
The QR code contains the same secret.

On October 30, 2026, the enabled Intuit account showed an Authenticator **Remove**
option, with no option to reveal the existing key. If the original key was not
saved and the authenticator app cannot show/export it, repeat enrollment:

1. Keep the signed-in Intuit session open and ensure the existing recovery method
   is accessible.
2. The account owner removes the old authenticator in **Sign in & security** and
   selects **Set up authenticator app** again. This is a credential change that
   the owner must complete; do not automate the enrollment UI.
3. While the new QR code is displayed, copy its **manual secret key** directly to
   the Azure Key Vault entry below. Do not paste the key, QR image, or an account
   export into chat, source code, issue trackers, or command-line arguments.
4. Scan that same new QR code with the phone app, enter its six-digit code into
   Intuit, and finish enrollment. The phone and service must use the same new key.
5. Verify the account shows the authenticator as enabled. The prior phone entry
   will no longer generate valid codes after the secret is replaced.

Microsoft Authenticator does not offer a supported TOTP-key export to other apps;
its backup/restore feature is different from displaying a setup key. Other apps
may have their own export mechanisms; do not export unrelated account secrets.

## Save the key in Azure

In the Azure portal, open Key Vault **qb-sync-kv** (resource group
**cabbagepatchkids**) → **Objects → Secrets → Generate/Import**:

- Name: `qb-totp-secret`
- Value: the manual setup key from the enrollment screen
- Enabled: Yes

Save it directly there, then clear it from the clipboard. Spaces and letter case
in the displayed Base32 key are accepted. The service uses the standard Intuit
six-digit SHA-1 / 30-second TOTP configuration.

The live Container App had no managed identity or Key Vault references when
inspected on October 30, 2026. Saving the key alone does **not** enable automatic
authentication. It still requires the code deployment and identity/reference
configuration below.

## Wire the service after deployment

Deploy an image containing `app.py`, `invoice_webhook.py`, and `qb_auth.py`.
Then run the following non-secret commands with appropriate Azure permissions:

```bash
QB_TOTP_PRINCIPAL_ID="$(az containerapp identity assign \
  --name qb-sync --resource-group cabbagepatchkids \
  --system-assigned --query principalId --output tsv)"
QB_TOTP_VAULT_ID="$(az keyvault show --name qb-sync-kv --query id --output tsv)"

az role assignment create \
  --assignee-object-id "$QB_TOTP_PRINCIPAL_ID" \
  --assignee-principal-type ServicePrincipal \
  --role 'Key Vault Secrets User' \
  --scope "$QB_TOTP_VAULT_ID/secrets/qb-totp-secret" \
  --output none

az containerapp secret set \
  --name qb-sync --resource-group cabbagepatchkids \
  --secrets 'qb-totp-secret=keyvaultref:https://qb-sync-kv.vault.azure.net/secrets/qb-totp-secret,identityref:system' \
  --output none

az containerapp update \
  --name qb-sync --resource-group cabbagepatchkids \
  --set-env-vars QB_TOTP_SECRET=secretref:qb-totp-secret \
  --output none
```

Role assignment propagation can take time; resolve access failures before retrying
the secret-reference step. Use secret-scoped access for this identity. Do not
print secret values or run `az containerapp secret list --show-values`.

## Behavior and validation

- Without `QB_TOTP_SECRET`, the existing manual SMS flow remains available.
- With `QB_TOTP_SECRET`, a login challenge attempts the authenticator method,
  including through a visible alternate-method chooser. It submits one generated
  code and waits for the signed-in QuickBooks URL.
- If Intuit offers only SMS, a CAPTCHA, an unsupported form, or rejects the code,
  login stops with an explicit error. It does not send SMS automatically or
  repeatedly guess codes.
- The authenticator helper does not log secrets, codes, or page text, and restricts
  submission to HTTPS `accounts.intuit.com` outside account-manager enrollment.
- The service has access to both the password and second-factor key. Key Vault
  restricts access to the key; it does not preserve phone-only factor separation.

Offline tests use the public RFC 6238 test key and mocked page interactions:

```bash
python -m unittest test_qb_auth -v
```

Live acceptance still requires an actual challenge from the Azure login. A
password-only login does not validate TOTP submission, and passing offline tests
does not establish compatibility with every Intuit screen. Test authentication
before a full sync; code deployment, key provisioning, and verified bank-balance
writes are separate milestones. Reusing browser sessions is outside this change.

References:

- [Intuit verification methods](https://quickbooks.intuit.com/learn-support/en-us/help-article/security-risk/verify-account-multi-factor-authentication/L2Xp0GNiT_US_en_US)
- [TOTP / RFC 6238](https://www.rfc-editor.org/rfc/rfc6238.html)
- [Azure Container Apps Key Vault references](https://learn.microsoft.com/en-us/azure/container-apps/manage-secrets)
- [Microsoft Authenticator backup/restore](https://support.microsoft.com/en-us/authenticator/back-up-your-accounts-in-microsoft-authenticator)
