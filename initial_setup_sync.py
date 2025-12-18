#!/usr/bin/env python3
"""
QuickBooks Online to QuickBase Sync Script
- Multi-entity OAuth token management
- Exports all QB objects to QuickBase tables
- One-time OAuth consent per company, then automated refresh

Usage:
    1. First run: python qb_to_quickbase_sync.py --setup (opens browser for OAuth)
    2. Subsequent runs: python qb_to_quickbase_sync.py --sync
    3. Add new company: python qb_to_quickbase_sync.py --add-company
"""

import os
import json
import time
import logging
import requests
import webbrowser
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode, parse_qs, urlparse
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, asdict
import base64

# Load .env file if present (before any config access)
try:
    from dotenv import load_dotenv
    load_dotenv()  # Loads from .env in current directory
except ImportError:
    pass  # dotenv not installed, rely on actual env vars

# =============================================================================
# Configuration
# =============================================================================

# QuickBooks OAuth endpoints
QB_AUTH_URL = "https://appcenter.intuit.com/connect/oauth2"
QB_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QB_API_BASE = "https://quickbooks.api.intuit.com/v3/company"

# QB API entities to sync (add/remove as needed)
QB_ENTITIES = [
    "Account",
    "Bill",
    "BillPayment",
    "Budget",
    "Class",
    "CompanyInfo",
    "CreditMemo",
    "Customer",
    "Department",
    "Deposit",
    "Employee",
    "Estimate",
    "Invoice",
    "Item",
    "JournalEntry",
    "Payment",
    "PaymentMethod",
    "Purchase",
    "PurchaseOrder",
    "RefundReceipt",
    "SalesReceipt",
    "TaxCode",
    "TaxRate",
    "Term",
    "TimeActivity",
    "Transfer",
    "Vendor",
    "VendorCredit",
]

# Local paths
CONFIG_DIR = Path.home() / ".qb_quickbase_sync"
TOKEN_FILE = CONFIG_DIR / "tokens.json"
CONFIG_FILE = CONFIG_DIR / "config.json"
LOG_FILE = CONFIG_DIR / "sync.log"

# OAuth redirect - using registered domain (manual code copy flow)
# For production, this would be an Azure Function endpoint
REDIRECT_URI = "https://dispatchenergy.com/qb-callback"

# =============================================================================
# Logging Setup
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE) if LOG_FILE.parent.exists() else logging.StreamHandler(),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class QBToken:
    """OAuth token data for a single QB company"""
    realm_id: str
    company_name: str
    access_token: str
    refresh_token: str
    access_token_expiry: str  # ISO format
    refresh_token_expiry: str  # ISO format
    created_at: str
    last_refreshed: str

@dataclass 
class Config:
    """App configuration"""
    client_id: str
    client_secret: str
    quickbase_realm: str
    quickbase_token: str
    quickbase_app_id: str

# =============================================================================
# Token Storage
# =============================================================================

class TokenStore:
    """Manages persistent storage of OAuth tokens for multiple QB companies"""
    
    def __init__(self, token_file: Path = TOKEN_FILE):
        self.token_file = token_file
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        self._tokens: Dict[str, QBToken] = {}
        self._load()
    
    def _load(self):
        """Load tokens from disk"""
        if self.token_file.exists():
            try:
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
                    for realm_id, token_data in data.items():
                        self._tokens[realm_id] = QBToken(**token_data)
                logger.info(f"Loaded {len(self._tokens)} company tokens")
            except Exception as e:
                logger.error(f"Error loading tokens: {e}")
                self._tokens = {}
    
    def _save(self):
        """Save tokens to disk"""
        data = {realm_id: asdict(token) for realm_id, token in self._tokens.items()}
        with open(self.token_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get(self, realm_id: str) -> Optional[QBToken]:
        """Get token for a specific company"""
        return self._tokens.get(realm_id)
    
    def get_all(self) -> Dict[str, QBToken]:
        """Get all stored tokens"""
        return self._tokens.copy()
    
    def save_token(self, token: QBToken):
        """Save or update a token"""
        self._tokens[token.realm_id] = token
        self._save()
        logger.info(f"Saved token for {token.company_name} ({token.realm_id})")
    
    def remove(self, realm_id: str):
        """Remove a token"""
        if realm_id in self._tokens:
            del self._tokens[realm_id]
            self._save()

# =============================================================================
# OAuth Handler
# =============================================================================

class QBOAuth:
    """Handles QuickBooks OAuth 2.0 flow"""
    
    def __init__(self, client_id: str, client_secret: str, token_store: TokenStore):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_store = token_store
    
    def get_auth_url(self) -> str:
        """Generate OAuth authorization URL"""
        params = {
            'client_id': self.client_id,
            'response_type': 'code',
            'scope': 'com.intuit.quickbooks.accounting',
            'redirect_uri': REDIRECT_URI,
            'state': 'security_token'
        }
        return f"{QB_AUTH_URL}?{urlencode(params)}"
    
    def authorize_company(self) -> Optional[QBToken]:
        """Run OAuth flow to authorize a new company (manual code entry)"""
        auth_url = self.get_auth_url()
        
        print("\n" + "="*60)
        print("QuickBooks Authorization")
        print("="*60)
        print("\n1. Opening browser for authorization...")
        print("   (If browser doesn't open, copy this URL):\n")
        print(f"   {auth_url}\n")
        
        webbrowser.open(auth_url)
        
        print("2. After authorizing, you'll be redirected to a page that may not load.")
        print("   That's OK! Look at the URL in your browser address bar.\n")
        print("   It will look like:")
        print("   https://dispatchenergy.com/qb-callback?code=XXXXX&state=...&realmId=YYYYY\n")
        
        print("3. Copy the ENTIRE URL from your browser and paste it here:\n")
        
        callback_url = input("   Paste URL: ").strip()
        
        # Parse the URL to extract code and realmId
        try:
            parsed = urlparse(callback_url)
            params = parse_qs(parsed.query)
            
            code = params.get('code', [None])[0]
            realm_id = params.get('realmId', [None])[0]
            
            if not code:
                logger.error("No authorization code found in URL")
                return None
            if not realm_id:
                logger.error("No realmId found in URL")
                return None
                
        except Exception as e:
            logger.error(f"Failed to parse callback URL: {e}")
            return None
        
        # Exchange code for tokens
        return self._exchange_code(code, realm_id)
    
    def _exchange_code(self, code: str, realm_id: str) -> Optional[QBToken]:
        """Exchange authorization code for access/refresh tokens"""
        auth_header = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()
        
        response = requests.post(
            QB_TOKEN_URL,
            headers={
                'Authorization': f'Basic {auth_header}',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
            },
            data={
                'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': REDIRECT_URI
            }
        )
        
        if response.status_code != 200:
            logger.error(f"Token exchange failed: {response.text}")
            return None
        
        data = response.json()
        now = datetime.utcnow()
        
        # Get company name
        company_name = self._get_company_name(data['access_token'], realm_id) or f"Company_{realm_id}"
        
        token = QBToken(
            realm_id=realm_id,
            company_name=company_name,
            access_token=data['access_token'],
            refresh_token=data['refresh_token'],
            access_token_expiry=(now + timedelta(seconds=data.get('expires_in', 3600))).isoformat(),
            refresh_token_expiry=(now + timedelta(days=100)).isoformat(),
            created_at=now.isoformat(),
            last_refreshed=now.isoformat()
        )
        
        self.token_store.save_token(token)
        logger.info(f"Successfully authorized: {company_name} ({realm_id})")
        return token
    
    def _get_company_name(self, access_token: str, realm_id: str) -> Optional[str]:
        """Fetch company name from QB API"""
        try:
            response = requests.get(
                f"{QB_API_BASE}/{realm_id}/companyinfo/{realm_id}",
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Accept': 'application/json'
                }
            )
            if response.status_code == 200:
                return response.json().get('CompanyInfo', {}).get('CompanyName')
        except Exception as e:
            logger.warning(f"Could not fetch company name: {e}")
        return None
    
    def refresh_token(self, token: QBToken) -> Optional[QBToken]:
        """Refresh an access token"""
        auth_header = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()
        
        response = requests.post(
            QB_TOKEN_URL,
            headers={
                'Authorization': f'Basic {auth_header}',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
            },
            data={
                'grant_type': 'refresh_token',
                'refresh_token': token.refresh_token
            }
        )
        
        if response.status_code != 200:
            logger.error(f"Token refresh failed for {token.company_name}: {response.text}")
            return None
        
        data = response.json()
        now = datetime.utcnow()
        
        updated_token = QBToken(
            realm_id=token.realm_id,
            company_name=token.company_name,
            access_token=data['access_token'],
            refresh_token=data['refresh_token'],
            access_token_expiry=(now + timedelta(seconds=data.get('expires_in', 3600))).isoformat(),
            refresh_token_expiry=(now + timedelta(days=100)).isoformat(),
            created_at=token.created_at,
            last_refreshed=now.isoformat()
        )
        
        self.token_store.save_token(updated_token)
        logger.info(f"Refreshed token for {token.company_name}")
        return updated_token
    
    def ensure_valid_token(self, token: QBToken) -> Optional[QBToken]:
        """Ensure token is valid, refresh if needed (15 min buffer)"""
        expiry = datetime.fromisoformat(token.access_token_expiry)
        if datetime.utcnow() + timedelta(minutes=15) >= expiry:
            logger.info(f"Token expiring soon for {token.company_name}, refreshing...")
            return self.refresh_token(token)
        return token
    
    def refresh_all_expiring(self, hours_threshold: int = 48):
        """Proactively refresh any tokens expiring within threshold"""
        threshold = datetime.utcnow() + timedelta(hours=hours_threshold)
        for realm_id, token in self.token_store.get_all().items():
            expiry = datetime.fromisoformat(token.access_token_expiry)
            if expiry < threshold:
                logger.info(f"Proactively refreshing token for {token.company_name}")
                self.refresh_token(token)

# =============================================================================
# QuickBooks API Client
# =============================================================================

class QBClient:
    """QuickBooks API client for a single company"""
    
    def __init__(self, token: QBToken, oauth: QBOAuth):
        self.token = token
        self.oauth = oauth
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with valid token"""
        self.token = self.oauth.ensure_valid_token(self.token)
        return {
            'Authorization': f'Bearer {self.token.access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
    
    def query(self, entity: str, select: str = "*", where: str = None, 
              max_results: int = 1000, start_position: int = 1) -> List[Dict]:
        """Query QB entity with pagination"""
        all_results = []
        
        while True:
            query = f"SELECT {select} FROM {entity}"
            if where:
                query += f" WHERE {where}"
            query += f" STARTPOSITION {start_position} MAXRESULTS {max_results}"
            
            url = f"{QB_API_BASE}/{self.token.realm_id}/query"
            params = {'query': query}
            
            response = requests.get(url, headers=self._get_headers(), params=params)
            
            if response.status_code != 200:
                logger.error(f"Query failed for {entity}: {response.text}")
                break
            
            data = response.json()
            query_response = data.get('QueryResponse', {})
            entities = query_response.get(entity, [])
            
            if not entities:
                break
            
            all_results.extend(entities)
            
            # Check if more results
            if len(entities) < max_results:
                break
            
            start_position += max_results
            time.sleep(0.1)  # Rate limiting courtesy
        
        return all_results
    
    def get_all_entities(self, entity: str) -> List[Dict]:
        """Get all records for an entity type"""
        logger.info(f"Fetching {entity} from {self.token.company_name}...")
        try:
            results = self.query(entity)
            logger.info(f"  Found {len(results)} {entity} records")
            return results
        except Exception as e:
            logger.error(f"  Error fetching {entity}: {e}")
            return []

# =============================================================================
# QuickBase Client
# =============================================================================

class QuickBaseClient:
    """QuickBase API client"""
    
    def __init__(self, realm: str, token: str, app_id: str):
        self.realm = realm
        self.token = token
        self.app_id = app_id
        self.base_url = f"https://api.quickbase.com/v1"
        self._table_cache: Dict[str, str] = {}  # name -> table_id
    
    def _get_headers(self) -> Dict[str, str]:
        return {
            'QB-Realm-Hostname': f'{self.realm}.quickbase.com',
            'Authorization': f'QB-USER-TOKEN {self.token}',
            'Content-Type': 'application/json'
        }
    
    def get_or_create_table(self, table_name: str, fields: List[Dict]) -> str:
        """Get existing table or create new one"""
        if table_name in self._table_cache:
            return self._table_cache[table_name]
        
        # Check if table exists
        response = requests.get(
            f"{self.base_url}/tables",
            headers=self._get_headers(),
            params={'appId': self.app_id}
        )
        
        if response.status_code == 200:
            for table in response.json():
                if table['name'] == table_name:
                    self._table_cache[table_name] = table['id']
                    return table['id']
        
        # Create table
        response = requests.post(
            f"{self.base_url}/tables",
            headers=self._get_headers(),
            params={'appId': self.app_id},
            json={
                'name': table_name,
                'description': f'QuickBooks {table_name} sync'
            }
        )
        
        if response.status_code in (200, 201):
            table_id = response.json()['id']
            self._table_cache[table_name] = table_id
            logger.info(f"Created table: {table_name} ({table_id})")
            
            # Add fields
            self._create_fields(table_id, fields)
            return table_id
        else:
            raise Exception(f"Failed to create table {table_name}: {response.text}")
    
    def _create_fields(self, table_id: str, fields: List[Dict]):
        """Create fields for a table"""
        for field in fields:
            response = requests.post(
                f"{self.base_url}/fields",
                headers=self._get_headers(),
                params={'tableId': table_id},
                json=field
            )
            if response.status_code not in (200, 201):
                logger.warning(f"Failed to create field {field.get('label')}: {response.text}")
    
    def upsert_records(self, table_id: str, records: List[Dict], 
                       key_field_id: int, field_mapping: Dict[str, int]):
        """Upsert records to QuickBase table"""
        if not records:
            return
        
        # Transform records to QB format
        qb_records = []
        for record in records:
            qb_record = {}
            for qb_field, fid in field_mapping.items():
                if qb_field in record:
                    value = record[qb_field]
                    if value is not None:
                        qb_record[str(fid)] = {'value': value}
            if qb_record:
                qb_records.append(qb_record)
        
        # Batch upsert (QuickBase limit is 10000 per request)
        batch_size = 5000
        for i in range(0, len(qb_records), batch_size):
            batch = qb_records[i:i + batch_size]
            response = requests.post(
                f"{self.base_url}/records",
                headers=self._get_headers(),
                json={
                    'to': table_id,
                    'data': batch,
                    'mergeFieldId': key_field_id,
                    'fieldsToReturn': [key_field_id]
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"  Upserted batch: {result.get('metadata', {}).get('totalNumberOfRecordsProcessed', 0)} records")
            else:
                logger.error(f"  Upsert failed: {response.text}")

# =============================================================================
# Sync Engine
# =============================================================================

class SyncEngine:
    """Orchestrates sync between QuickBooks and QuickBase"""
    
    # Field type mapping from QB to QuickBase
    QB_TO_QUICKBASE_TYPES = {
        'Id': 'text',
        'SyncToken': 'text',
        'Name': 'text',
        'FullyQualifiedName': 'text',
        'Active': 'checkbox',
        'Balance': 'currency',
        'TotalAmt': 'currency',
        'Amount': 'currency',
        'Rate': 'numeric',
        'Qty': 'numeric',
        'TxnDate': 'date',
        'DueDate': 'date',
        'CreateTime': 'datetime',
        'MetaData.CreateTime': 'datetime',
        'MetaData.LastUpdatedTime': 'datetime',
    }
    
    def __init__(self, oauth: QBOAuth, qb_client: QuickBaseClient):
        self.oauth = oauth
        self.qb_client = qb_client
    
    def _infer_field_type(self, field_name: str, sample_value: Any) -> str:
        """Infer QuickBase field type from field name and sample value"""
        # Check known mappings
        if field_name in self.QB_TO_QUICKBASE_TYPES:
            return self.QB_TO_QUICKBASE_TYPES[field_name]
        
        # Infer from name patterns
        lower_name = field_name.lower()
        if 'date' in lower_name or 'time' in lower_name:
            return 'date'
        if 'amt' in lower_name or 'amount' in lower_name or 'balance' in lower_name or 'price' in lower_name:
            return 'currency'
        if 'qty' in lower_name or 'quantity' in lower_name or 'count' in lower_name:
            return 'numeric'
        if 'active' in lower_name or 'taxable' in lower_name:
            return 'checkbox'
        if lower_name.endswith('ref'):
            return 'text'  # Reference fields stored as text
        
        # Infer from value type
        if sample_value is not None:
            if isinstance(sample_value, bool):
                return 'checkbox'
            if isinstance(sample_value, (int, float)):
                return 'numeric'
            if isinstance(sample_value, dict):
                return 'text'  # Nested objects serialized as JSON
        
        return 'text'
    
    def _flatten_record(self, record: Dict, prefix: str = '') -> Dict:
        """Flatten nested QB record to single-level dict"""
        flat = {}
        for key, value in record.items():
            full_key = f"{prefix}{key}" if prefix else key
            
            if isinstance(value, dict):
                # Handle reference objects (e.g., CustomerRef)
                if 'value' in value and 'name' in value:
                    flat[f"{full_key}_Id"] = value['value']
                    flat[f"{full_key}_Name"] = value['name']
                elif 'value' in value:
                    flat[full_key] = value['value']
                else:
                    # Recurse for nested objects like MetaData
                    flat.update(self._flatten_record(value, f"{full_key}_"))
            elif isinstance(value, list):
                # Serialize lists as JSON
                flat[full_key] = json.dumps(value)
            else:
                flat[full_key] = value
        
        return flat
    
    def _build_field_definitions(self, records: List[Dict]) -> List[Dict]:
        """Build QuickBase field definitions from sample records"""
        if not records:
            return []
        
        # Sample first few records for field discovery
        sample = records[:min(10, len(records))]
        all_fields = {}
        
        for record in sample:
            flat = self._flatten_record(record)
            for key, value in flat.items():
                if key not in all_fields:
                    all_fields[key] = value
        
        # Build field definitions
        fields = []
        for field_name, sample_value in all_fields.items():
            field_type = self._infer_field_type(field_name, sample_value)
            fields.append({
                'label': field_name,
                'fieldType': field_type
            })
        
        return fields
    
    def sync_entity(self, client: QBClient, entity: str, realm_id: str):
        """Sync a single entity type from one QB company"""
        records = client.get_all_entities(entity)
        if not records:
            return
        
        # Flatten records
        flat_records = [self._flatten_record(r) for r in records]
        
        # Add source tracking fields
        for r in flat_records:
            r['_QB_RealmId'] = realm_id
            r['_QB_SyncTime'] = datetime.utcnow().isoformat()
        
        # Build table name (prefix with company for multi-entity)
        table_name = f"QB_{entity}"
        
        # Get/create table with inferred schema
        fields = self._build_field_definitions(flat_records)
        fields.extend([
            {'label': '_QB_RealmId', 'fieldType': 'text'},
            {'label': '_QB_SyncTime', 'fieldType': 'datetime'},
            {'label': '_QB_UniqueKey', 'fieldType': 'text'}  # Composite key: realm_id + entity_id
        ])
        
        # Create unique key for upsert
        for r in flat_records:
            r['_QB_UniqueKey'] = f"{realm_id}_{r.get('Id', '')}"
        
        # Note: In production, you'd cache table_id and field_ids
        # This is simplified for the example
        logger.info(f"  Would upsert {len(flat_records)} records to {table_name}")
        
        # Uncomment to actually create tables and sync:
        # table_id = self.qb_client.get_or_create_table(table_name, fields)
        # self.qb_client.upsert_records(table_id, flat_records, key_field_id, field_mapping)
    
    def sync_all(self, entities: List[str] = None):
        """Sync all entities from all connected QB companies"""
        entities = entities or QB_ENTITIES
        tokens = self.oauth.token_store.get_all()
        
        if not tokens:
            logger.warning("No QB companies connected. Run with --add-company first.")
            return
        
        logger.info(f"Starting sync for {len(tokens)} companies, {len(entities)} entity types")
        
        for realm_id, token in tokens.items():
            logger.info(f"\n{'='*60}")
            logger.info(f"Syncing: {token.company_name} ({realm_id})")
            logger.info(f"{'='*60}")
            
            client = QBClient(token, self.oauth)
            
            for entity in entities:
                try:
                    self.sync_entity(client, entity, realm_id)
                except Exception as e:
                    logger.error(f"Error syncing {entity}: {e}")
                    continue
        
        logger.info("\nSync complete!")

# =============================================================================
# Configuration Management
# =============================================================================

def load_config() -> Optional[Config]:
    """Load configuration from environment variables or config file
    
    Environment variables (preferred):
        QB_CLIENT_ID        - QuickBooks OAuth Client ID
        QB_CLIENT_SECRET    - QuickBooks OAuth Client Secret
        QUICKBASE_REALM     - QuickBase realm (e.g., 'mycompany')
        QUICKBASE_TOKEN     - QuickBase user token
        QUICKBASE_APP_ID    - QuickBase app ID
    
    Falls back to ~/.qb_quickbase_sync/config.json if env vars not set
    """
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Try environment variables first (preferred)
    env_config = {
        'client_id': os.environ.get('QB_CLIENT_ID'),
        'client_secret': os.environ.get('QB_CLIENT_SECRET'),
        'quickbase_realm': os.environ.get('QUICKBASE_REALM'),
        'quickbase_token': os.environ.get('QUICKBASE_TOKEN'),
        'quickbase_app_id': os.environ.get('QUICKBASE_APP_ID')
    }
    
    if all(env_config.values()):
        logger.info("Loaded configuration from environment variables")
        return Config(**env_config)
    
    # Try config file as fallback
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r') as f:
            data = json.load(f)
            logger.info(f"Loaded configuration from {CONFIG_FILE}")
            return Config(**data)
    
    # Report what's missing
    missing_env = [k for k, v in {
        'QB_CLIENT_ID': env_config['client_id'],
        'QB_CLIENT_SECRET': env_config['client_secret'],
        'QUICKBASE_REALM': env_config['quickbase_realm'],
        'QUICKBASE_TOKEN': env_config['quickbase_token'],
        'QUICKBASE_APP_ID': env_config['quickbase_app_id']
    }.items() if not v]
    
    if missing_env:
        logger.error(f"Missing environment variables: {', '.join(missing_env)}")
    
    return None

# =============================================================================
# CLI
# =============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='QuickBooks to QuickBase Sync',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables (required):
  QB_CLIENT_ID        QuickBooks OAuth Client ID
  QB_CLIENT_SECRET    QuickBooks OAuth Client Secret
  QUICKBASE_REALM     QuickBase realm (e.g., 'mycompany')
  QUICKBASE_TOKEN     QuickBase user token
  QUICKBASE_APP_ID    QuickBase app ID

Examples:
  # Add first QB company (opens browser for OAuth consent)
  python qb_to_quickbase_sync.py --add-company

  # Add more companies
  python qb_to_quickbase_sync.py --add-company

  # List all connected companies
  python qb_to_quickbase_sync.py --list

  # Run sync for all companies
  python qb_to_quickbase_sync.py --sync

  # Sync specific entities only
  python qb_to_quickbase_sync.py --sync --entities Invoice Customer Payment
        """
    )
    parser.add_argument('--add-company', action='store_true', help='Add new QB company (OAuth flow)')
    parser.add_argument('--sync', action='store_true', help='Run sync')
    parser.add_argument('--list', action='store_true', help='List connected companies')
    parser.add_argument('--refresh-all', action='store_true', help='Refresh all tokens')
    parser.add_argument('--entities', nargs='+', help='Specific entities to sync')
    parser.add_argument('--dry-run', action='store_true', help='Show what would sync without syncing')
    
    args = parser.parse_args()
    
    # Load config from env vars (or fallback to config file)
    config = load_config()
    if not config:
        print("\nConfiguration not found. Set environment variables:")
        print("  export QB_CLIENT_ID='your_client_id'")
        print("  export QB_CLIENT_SECRET='your_client_secret'")
        print("  export QUICKBASE_REALM='your_realm'")
        print("  export QUICKBASE_TOKEN='your_token'")
        print("  export QUICKBASE_APP_ID='your_app_id'")
        print("\nOr create ~/.qb_quickbase_sync/config.json")
        return
    
    token_store = TokenStore()
    oauth = QBOAuth(config.client_id, config.client_secret, token_store)
    
    # Add company
    if args.add_company:
        oauth.authorize_company()
        return
    
    # List companies
    if args.list:
        tokens = token_store.get_all()
        if not tokens:
            print("No companies connected. Run with --add-company first.")
        else:
            print(f"\nConnected QuickBooks Companies ({len(tokens)}):")
            for realm_id, token in tokens.items():
                expiry = datetime.fromisoformat(token.access_token_expiry)
                refresh_expiry = datetime.fromisoformat(token.refresh_token_expiry)
                
                if datetime.utcnow() < expiry:
                    status = "✓ Valid"
                elif datetime.utcnow() < refresh_expiry:
                    status = "↻ Needs refresh"
                else:
                    status = "✗ Expired (re-auth needed)"
                
                print(f"  - {token.company_name} ({realm_id}): {status}")
                print(f"      Last synced: {token.last_refreshed}")
        return
    
    # Refresh all
    if args.refresh_all:
        oauth.refresh_all_expiring(hours_threshold=0)  # Force refresh all
        return
    
    # Sync (default action if no flags)
    if args.sync or not any([args.add_company, args.list, args.refresh_all]):
        qb_client = QuickBaseClient(
            realm=config.quickbase_realm,
            token=config.quickbase_token,
            app_id=config.quickbase_app_id
        )
        
        engine = SyncEngine(oauth, qb_client)
        engine.sync_all(entities=args.entities)

if __name__ == '__main__':
    main()
