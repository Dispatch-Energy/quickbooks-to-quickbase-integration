# QuickBooks → QuickBase Sync

Azure Function that syncs bank accounts and pending transactions from QuickBooks Online to QuickBase.

## Architecture

- **Timer Trigger**: Runs daily at 6 AM MST (1 PM UTC)
- **HTTP Trigger**: Manual sync via POST to `/api/sync`
- **Playwright**: Headless Chromium for QuickBooks login
- **Container**: Custom Docker image with Chromium dependencies

## Estimated Cost

~$8-15/month:
- Function App (Premium EP1): ~$5-10/month
- Container Registry (Basic): ~$5/month
- Key Vault: ~$0.03/month
- Storage: ~$0.10/month

## Setup

### Prerequisites

- Azure CLI installed and logged in (`az login`)
- GitHub repository created
- QuickBooks account (without 2FA)
- QuickBase user token

### 1. Run Azure Setup

```bash
chmod +x setup-azure.sh
./setup-azure.sh
```

This creates:
- Resource Group
- Storage Account
- Container Registry
- Key Vault (with secrets)
- Function App (Premium plan with container support)

### 2. Create GitHub Service Principal

```bash
az ad sp create-for-rbac --name 'qb-sync-github' \
  --role contributor \
  --scopes /subscriptions/$(az account show --query id -o tsv)/resourceGroups/qb-sync-rg \
  --sdk-auth
```

Copy the JSON output.

### 3. Add GitHub Secret

In your GitHub repo:
1. Go to **Settings** → **Secrets and variables** → **Actions**
2. Add new secret: `AZURE_CREDENTIALS`
3. Paste the JSON from step 2

### 4. Push and Deploy

```bash
git add .
git commit -m "Initial deploy"
git push origin main
```

GitHub Actions will build the container and deploy to Azure.

## Manual Trigger

Get the function key from Azure Portal → Function App → Functions → qb_sync_manual → Function Keys

```bash
curl -X POST "https://qb-sync-func.azurewebsites.net/api/sync?code=YOUR_FUNCTION_KEY"
```

## Monitoring

- **Logs**: Azure Portal → Function App → Log Stream
- **Metrics**: Azure Portal → Function App → Metrics
- **Invocations**: Azure Portal → Function App → Functions → qb_sync_timer → Monitor

## Troubleshooting

### CAPTCHA Triggered

If login fails due to CAPTCHA, the Azure IP may be flagged. Options:
1. Wait 24 hours and retry
2. Login manually from same region to "trust" the IP pattern
3. Consider Azure VPN Gateway for consistent IP

### 2FA/MFA Required

This function cannot handle 2FA. The QuickBooks account must have 2FA disabled.

### Missing Configuration

Check Function App → Configuration → Application Settings for:
- `QB_USERNAME`
- `QB_PASSWORD`
- `QUICKBASE_TOKEN`
- `QUICKBASE_REALM`
- `ACCOUNTS_TABLE_ID`
- `TRANSACTIONS_TABLE_ID`

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt
playwright install chromium

# Create local.settings.json
cat > local.settings.json << EOF
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "QB_USERNAME": "your-email",
    "QB_PASSWORD": "your-password",
    "QUICKBASE_TOKEN": "your-token",
    "QUICKBASE_REALM": "dispatchenergy",
    "ACCOUNTS_TABLE_ID": "bxxxxxxxxx",
    "TRANSACTIONS_TABLE_ID": "bxxxxxxxxx"
  }
}
EOF

# Run locally
func start
```

## Files

```
├── .github/
│   └── workflows/
│       └── deploy.yml      # GitHub Actions CI/CD
├── function_app.py         # Main function code
├── Dockerfile              # Container with Chromium
├── requirements.txt        # Python dependencies
├── host.json               # Function host config
├── setup-azure.sh          # One-time Azure setup
└── README.md
```
