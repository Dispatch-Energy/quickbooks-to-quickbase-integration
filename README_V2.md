# QuickBooks → QuickBase Sync v2

## Changes from v1

### 1. Bank Balances (Daily Snapshots)
Added a new child table sync for **Bank Balances** that creates daily balance snapshots:

- **Table**: Bank Balance (you need to create this in QuickBase)
- **Fields**:
  - Field 6: Balance (Currency) - the bank balance amount
  - Field 7: Date Added (Date) - the snapshot date
  - Field 8: Related Bank Account (Numeric reference to Bank Account)

**Behavior**:
- Creates one balance record per account per day
- Checks for existing records before inserting to prevent duplicates
- Uses bank balance if available, falls back to QuickBooks balance
- Runs after account sync, before transaction sync

### 2. GL Integration
Optionally integrates the OAuth-based GL sync (`qb_to_quickbase_sync.py`) into the workflow.

## Environment Variables

### Bank Feeds (Required)
```bash
export QB_USERNAME="it@dispatchenergy.com"
export QB_PASSWORD="your-password"
export QUICKBASE_TOKEN="your-token"
export QUICKBASE_REALM="dispatchenergy"
export ACCOUNTS_TABLE_ID="bxxxxxxxx"
export TRANSACTIONS_TABLE_ID="bxxxxxxxx"
export BALANCES_TABLE_ID="bxxxxxxxx"  # NEW
```

### GL Sync (Optional)
```bash
export QB_CLIENT_ID="your-client-id"
export QB_CLIENT_SECRET="your-client-secret"
export QUICKBASE_APP_ID="bxxxxxxxx"
```

## Usage

### Local Testing Script (qb_sync_v2.py)

```bash
# Bank feeds only (accounts, balances, transactions)
python qb_sync_v2.py

# Bank feeds + GL sync
python qb_sync_v2.py --with-gl

# GL sync only
python qb_sync_v2.py --gl-only

# Skip bank balances
python qb_sync_v2.py --skip-balances
```

### Azure Container App (app_v2.py)

Start the server:
```bash
uvicorn app_v2:app --host 0.0.0.0 --port 8080
```

Endpoints:
- `POST /sync` - Bank feeds sync (accounts, balances, transactions)
- `POST /sync-gl` - GL sync only (OAuth-based)
- `POST /sync-all` - Both bank feeds + GL sync
- `POST /code` - Submit SMS verification code
- `GET /screenshot` - View latest screenshot
- `GET /health` - Health check

Example requests:
```bash
# Trigger bank feeds sync
curl -X POST http://localhost:8080/sync

# Trigger bank feeds sync, skip balances
curl -X POST http://localhost:8080/sync \
  -H "Content-Type: application/json" \
  -d '{"skip_balances": true}'

# Trigger GL sync
curl -X POST http://localhost:8080/sync-gl

# Trigger full sync (bank feeds + GL)
curl -X POST http://localhost:8080/sync-all
```

## QuickBase Table Setup

### Bank Balance Table

Create a new table in QuickBase with these fields:

| Field ID | Field Name | Type | Notes |
|----------|------------|------|-------|
| 3 | Record ID# | Numeric (built-in) | |
| 6 | Balance | Currency | The bank balance amount |
| 7 | Date Added | Date | The snapshot date |
| 8 | Related Bank Account | Numeric | Reference to Bank Account table |

Set up a relationship:
1. Make "Related Bank Account" a reference field to the Bank Account table
2. Use Record ID# as the reference target

## Testing Steps

1. **Set environment variables** for QuickBase and QuickBooks credentials

2. **Create the Bank Balance table** in QuickBase with the schema above

3. **Test locally first**:
   ```bash
   python qb_sync_v2.py
   ```

4. **Verify in QuickBase**:
   - Check Bank Account table was updated
   - Check Bank Balance table has new records with today's date
   - Check Bank Transaction table has pending transactions

5. **Deploy to Azure**:
   - Update the Container App with `app_v2.py`
   - Add `BALANCES_TABLE_ID` environment variable
   - Test via `/sync` endpoint

## Notes

- Bank balances are **inserts** (not upserts) - creates historical snapshots
- Duplicate prevention uses date + account combination check
- If balances already exist for today, sync is skipped for those accounts
- GL sync requires OAuth tokens to be set up first via `qb_to_quickbase_sync.py --add-company`