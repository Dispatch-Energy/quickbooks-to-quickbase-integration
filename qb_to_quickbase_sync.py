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
from datetime import datetime, timedelta, timezone

# Helper for timezone-aware UTC datetime
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def parse_datetime(dt_str: str) -> datetime:
    """Parse ISO datetime string, making it timezone-aware if needed"""
    dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
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

# QB API entities to sync (matched to QuickBase schema)
QB_ENTITIES = [
    "Account",
    "Bill",
    "BillPayment",
    "Class",
    "Customer",
    "Department",
    "Deposit",
    "Invoice",
    "Item",
    "JournalEntry",
    "Payment",
    "Purchase",
    "Transfer",
    "Vendor",
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
        now = utc_now()
        
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
        now = utc_now()
        
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
        expiry = parse_datetime(token.access_token_expiry)
        if utc_now() + timedelta(minutes=15) >= expiry:
            logger.info(f"Token expiring soon for {token.company_name}, refreshing...")
            return self.refresh_token(token)
        return token
    
    def refresh_all_expiring(self, hours_threshold: int = 48):
        """Proactively refresh any tokens expiring within threshold"""
        threshold = utc_now() + timedelta(hours=hours_threshold)
        for realm_id, token in self.token_store.get_all().items():
            expiry = parse_datetime(token.access_token_expiry)
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
        # Check if token needs refresh (without recursion)
        expiry = parse_datetime(self.token.access_token_expiry)
        if utc_now() + timedelta(minutes=15) >= expiry:
            logger.info(f"Token expiring soon for {self.token.company_name}, refreshing...")
            refreshed = self.oauth.refresh_token(self.token)
            if refreshed:
                self.token = refreshed
        
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
# QuickBase Schema Mapping (matches existing app: bvntqcqzm)
# =============================================================================

QUICKBASE_SCHEMA = {
    'app_id': 'bvntqcqzm',
    'tables': {
        'Entities': {
            'table_id': 'bvntqdiu3',
            'qb_entity': None,
            'key_field': 7,
            'fields': {
                'Entity Name': 6,
                'Quickbooks ID': 7,
                'Last Sync Time': 10,
            }
        },
        'Accounts': {
            'table_id': 'bvntqeriw',
            'qb_entity': 'Account',
            'unique_key_field': 15,
            'fields': {
                'Account Name': 6,
                'Account Number': 7,
                'Segment': 8,
                'Related Entity': 9,
                'Account Type': 10,
                'Account Subtype': 11,
                'Current Balance': 12,
                'QB_UniqueKey': 15,
            },
            'qb_mapping': {
                'Name': 6,
                'AcctNum': 7,
                'AccountType': 10,
                'AccountSubType': 11,
                'CurrentBalance': 12,
            }
        },
        'Customers': {
            'table_id': 'bvnw7w3wn',
            'qb_entity': 'Customer',
            'unique_key_field': 11,
            'fields': {
                'ID': 6,
                'Company Name': 7,
                'Primary Email Address': 8,
                'Balance': 9,
                'Related Entity': 10,
                'QB_UniqueKey': 11,
            },
            'qb_mapping': {
                'Id': 6,
                'CompanyName': 7,
                'PrimaryEmailAddr.Address': 8,
                'Balance': 9,
            }
        },
        'Vendors': {
            'table_id': 'bvnw8brfq',
            'qb_entity': 'Vendor',
            'unique_key_field': 11,
            'fields': {
                'Display Name': 6,
                'Company Name': 7,
                'Primary Email': 8,
                'Balance': 9,
                'Related Entity': 10,
                'QB_UniqueKey': 11,
            },
            'qb_mapping': {
                'DisplayName': 6,
                'CompanyName': 7,
                'PrimaryEmailAddr.Address': 8,
                'Balance': 9,
            }
        },
        'Items': {
            'table_id': 'bvnw8eb39',
            'qb_entity': 'Item',
            'unique_key_field': 11,
            'fields': {
                'Name': 6,
                'Type': 7,
                'Unit Price': 8,
                'On Hand': 9,
                'Active': 10,
                'QB_UniqueKey': 11,
            },
            'qb_mapping': {
                'Name': 6,
                'Type': 7,
                'UnitPrice': 8,
                'QtyOnHand': 9,
                'Active': 10,
            }
        },
        'Invoices': {
            'table_id': 'bvnw8muyw',
            'qb_entity': 'Invoice',
            'unique_key_field': 15,
            'fields': {
                'ID': 6,
                'Document Number': 7,
                'Transaction Date': 8,
                'Due Date': 9,
                'Total Amount': 10,
                'Balance': 11,
                'Related Entity': 14,
                'QB_UniqueKey': 15,
            },
            'qb_mapping': {
                'Id': 6,
                'DocNumber': 7,
                'TxnDate': 8,
                'DueDate': 9,
                'TotalAmt': 10,
                'Balance': 11,
            }
        },
        'Invoice Lines': {
            'table_id': 'bvnw8sq6r',
            'qb_entity': None,
            'parent_entity': 'Invoice',
            'unique_key_field': None,
            'parent_ref_field': 15,
            'fields': {
                'Items': 6,
                'Description': 7,
                'LineNum': 8,
                'Quantity': 9,
                'Unit Price': 10,
                'Amount': 11,
                'Related Invoice (ref)': 15,
            }
        },
        'Bills': {
            'table_id': 'bvnw8xkku',
            'qb_entity': 'Bill',
            'unique_key_field': 15,
            'fields': {
                'ID': 6,
                'Document Number': 7,
                'Transaction Date': 8,
                'Due Date': 9,
                'Total Amount': 10,
                'Balance': 11,
                'Related Entity': 12,
                'QB_UniqueKey': 15,
                'Account': 16,
            },
            'qb_mapping': {
                'Id': 6,
                'DocNumber': 7,
                'TxnDate': 8,
                'DueDate': 9,
                'TotalAmt': 10,
                'Balance': 11,
                'APAccountRef.name': 16,
            }
        },
        'Bill Line Items': {
            'table_id': 'bvnw82ykg',
            'qb_entity': None,
            'parent_entity': 'Bill',
            'unique_key_field': 18,
            'parent_ref_field': 15,
            'fields': {
                'Line Number': 6,
                'Description': 7,
                'Amount': 8,
                'Related Bill (ref)': 15,
                'QB_UniqueKey': 18,
            }
        },
        'Customer Payments': {
            'table_id': 'bvnw84pss',
            'qb_entity': 'Payment',
            'unique_key_field': 11,
            'fields': {
                'ID': 6,
                'Transaction Date': 7,
                'Total Amount': 8,
                'Payment Method': 9,
                'Related Line': 10,
                'QB_UniqueKey': 11,
            },
            'qb_mapping': {
                'Id': 6,
                'TxnDate': 7,
                'TotalAmt': 8,
                'PaymentMethodRef.name': 9,
            }
        },
        'Bill Payments': {
            'table_id': 'bvnw876nt',
            'qb_entity': 'BillPayment',
            'unique_key_field': 12,
            'fields': {
                'ID': 6,
                'Transaction Date': 7,
                'Total Amount': 8,
                'QB_UniqueKey': 12,
                'Related Bill Line Item (ref)': 13,
            },
            'qb_mapping': {
                'Id': 6,
                'TxnDate': 7,
                'TotalAmt': 8,
            }
        },
        'Journal Entries': {
            'table_id': 'bvnw89jdu',
            'qb_entity': 'JournalEntry',
            'unique_key_field': 14,
            'fields': {
                'ID': 6,
                'Document Number': 7,
                'Transaction Date': 8,
                'Total Amount': 9,
                'Private Note': 10,
                'Related Entity': 13,
                'QB_UniqueKey': 14,
                'Account': 15,
            },
            'qb_mapping': {
                'Id': 6,
                'DocNumber': 7,
                'TxnDate': 8,
                'TotalAmt': 9,
                'PrivateNote': 10,
            }
        },
        'Journal Entry Lines': {
            'table_id': 'bvnw9a5hk',
            'qb_entity': None,
            'parent_entity': 'JournalEntry',
            'unique_key_field': 16,
            'parent_ref_field': 13,
            'fields': {
                'LineNum': 6,
                'Posting Type': 7,
                'Amount': 8,
                'Related Journal Entry (ref)': 13,
                'QB_UniqueKey': 16,
                'Account': 17,
            }
        },
        'Purchases': {
            'table_id': 'bvnw9cme8',
            'qb_entity': 'Purchase',
            'unique_key_field': 13,
            'fields': {
                'ID': 6,
                'Transaction Date': 7,
                'Entity Ref ID': 8,
                'Total Amount': 9,
                'Payment Type': 10,
                'Related Entity': 11,
                'QB_UniqueKey': 13,
            },
            'qb_mapping': {
                'Id': 6,
                'TxnDate': 7,
                'EntityRef.value': 8,
                'TotalAmt': 9,
                'PaymentType': 10,
            }
        },
        'Deposits': {
            'table_id': 'bvnw9ec7q',
            'qb_entity': 'Deposit',
            'unique_key_field': 12,
            'fields': {
                'Transaction Date': 6,
                'Amount': 7,
                'To Account': 8,
                'Related Entity': 10,
                'QB_UniqueKey': 12,
            },
            'qb_mapping': {
                'TxnDate': 6,
                'TotalAmt': 7,
                'DepositToAccountRef.name': 8,
            }
        },
        'Transfers': {
            'table_id': 'bvnw9mzde',
            'qb_entity': 'Transfer',
            'unique_key_field': 11,
            'fields': {
                'Amount': 6,
                'From Account': 7,
                'To Account': 8,
                'Related Entity': 9,
                'QB_UniqueKey': 11,
            },
            'qb_mapping': {
                'Amount': 6,
                'FromAccountRef.name': 7,
                'ToAccountRef.name': 8,
            }
        },
        'Classes': {
            'table_id': 'bvnw9n637',
            'qb_entity': 'Class',
            'unique_key_field': None,
            'fields': {
                'ID': 6,
                'Name': 7,
                'Active': 8,
                'Related Entity': 9,
            },
            'qb_mapping': {
                'Id': 6,
                'Name': 7,
                'Active': 8,
            }
        },
        'Departments': {
            'table_id': 'bvnw9tx4v',
            'qb_entity': 'Department',
            'unique_key_field': None,
            'fields': {
                'Name': 6,
            },
            'qb_mapping': {
                'Name': 6,
            }
        },
    }
}


# =============================================================================
# QuickBase Client
# =============================================================================

class QuickBaseClient:
    """QuickBase API client"""
    
    UNIQUE_KEY_FIELD_NAME = "QB_UniqueKey"
    
    def __init__(self, realm: str, token: str, app_id: str = None):
        self.realm = realm
        self.token = token
        self.app_id = app_id or QUICKBASE_SCHEMA['app_id']
        self.base_url = "https://api.quickbase.com/v1"
        self._entity_record_cache: Dict[str, int] = {}  # realm_id -> record_id
        self._unique_key_field_cache: Dict[str, int] = {}  # table_id -> field_id
    
    def _get_headers(self) -> Dict[str, str]:
        return {
            'QB-Realm-Hostname': f'{self.realm}.quickbase.com',
            'Authorization': f'QB-USER-TOKEN {self.token}',
            'Content-Type': 'application/json'
        }
    
    def _get_or_create_unique_key_field(self, table_id: str, table_name: str) -> Optional[int]:
        """Find the QB_UniqueKey field for a table (must be created manually in QuickBase)"""
        if table_id in self._unique_key_field_cache:
            return self._unique_key_field_cache[table_id]
        
        # Query existing fields to find QB_UniqueKey
        response = requests.get(
            f"{self.base_url}/fields",
            headers=self._get_headers(),
            params={'tableId': table_id}
        )
        
        if response.status_code == 200:
            for field in response.json():
                if field.get('label') == self.UNIQUE_KEY_FIELD_NAME:
                    field_id = field['id']
                    self._unique_key_field_cache[table_id] = field_id
                    logger.info(f"  Found {self.UNIQUE_KEY_FIELD_NAME} field ({field_id}) in {table_name}")
                    return field_id
        
        # Field not found - log warning and continue without merge
        logger.warning(f"  {self.UNIQUE_KEY_FIELD_NAME} field not found in {table_name} - records will be inserted (no merge)")
        return None
    
    def get_or_create_entity(self, realm_id: str, company_name: str) -> int:
        """Get or create entity record, return QuickBase record ID"""
        if realm_id in self._entity_record_cache:
            return self._entity_record_cache[realm_id]
        
        table_id = QUICKBASE_SCHEMA['tables']['Entities']['table_id']
        
        # Query for existing entity
        response = requests.post(
            f"{self.base_url}/records/query",
            headers=self._get_headers(),
            json={
                'from': table_id,
                'select': [3, 7],  # Record ID#, Quickbooks ID
                'where': f"{{7.EX.'{realm_id}'}}"
            }
        )
        
        if response.status_code == 200:
            data = response.json().get('data', [])
            if data:
                record_id = data[0]['3']['value']
                self._entity_record_cache[realm_id] = record_id
                return record_id
        
        # Create new entity
        response = requests.post(
            f"{self.base_url}/records",
            headers=self._get_headers(),
            json={
                'to': table_id,
                'data': [{
                    '6': {'value': company_name},
                    '7': {'value': realm_id},
                    '10': {'value': utc_now().strftime('%Y-%m-%dT%H:%M:%SZ')}
                }]
            }
        )
        
        if response.status_code == 200:
            record_id = response.json()['metadata']['createdRecordIds'][0]
            self._entity_record_cache[realm_id] = record_id
            logger.info(f"Created entity record for {company_name}: {record_id}")
            return record_id
        else:
            raise Exception(f"Failed to create entity: {response.text}")
    
    def update_entity_sync_time(self, realm_id: str):
        """Update last sync time for entity"""
        if realm_id not in self._entity_record_cache:
            return
        
        record_id = self._entity_record_cache[realm_id]
        table_id = QUICKBASE_SCHEMA['tables']['Entities']['table_id']
        
        requests.post(
            f"{self.base_url}/records",
            headers=self._get_headers(),
            json={
                'to': table_id,
                'data': [{
                    '3': {'value': record_id},
                    '10': {'value': utc_now().strftime('%Y-%m-%dT%H:%M:%SZ')}
                }]
            }
        )
    
    def upsert_records(self, table_name: str, records: List[Dict], entity_record_id: int, realm_id: str) -> Dict:
        """Upsert records to a QuickBase table
        
        Args:
            table_name: Name of table in QUICKBASE_SCHEMA
            records: List of QB records (raw from API)
            entity_record_id: QuickBase record ID for the parent entity
            realm_id: QuickBooks realm ID for composite key
        
        Returns:
            Dict with counts of created/updated records
        """
        if not records:
            return {'created': 0, 'updated': 0}
        
        table_config = QUICKBASE_SCHEMA['tables'].get(table_name)
        if not table_config:
            logger.warning(f"Unknown table: {table_name}")
            return {'created': 0, 'updated': 0}
        
        table_id = table_config['table_id']
        qb_mapping = table_config.get('qb_mapping', {})
        entity_field = table_config['fields'].get('Related Entity')
        
        # Get unique key field from schema (or try to find it)
        unique_key_field_id = table_config.get('unique_key_field')
        if not unique_key_field_id:
            unique_key_field_id = self._get_or_create_unique_key_field(table_id, table_name)
        
        # Transform records
        qb_records = []
        for record in records:
            qb_record = {}
            
            # Map QB fields to QuickBase fields
            for qb_field, fid in qb_mapping.items():
                value = self._get_nested_value(record, qb_field)
                if value is not None:
                    qb_record[str(fid)] = {'value': value}
            
            # Add entity relationship
            if entity_field:
                qb_record[str(entity_field)] = {'value': entity_record_id}
            
            # Add composite unique key: {qb_id}_{realm_id}
            if unique_key_field_id:
                qb_id = record.get('Id', '')
                unique_key = f"{qb_id}_{realm_id}"
                qb_record[str(unique_key_field_id)] = {'value': unique_key}
            
            if qb_record:
                qb_records.append(qb_record)
        
        if not qb_records:
            return {'created': 0, 'updated': 0}
        
        # Batch upsert
        results = {'created': 0, 'updated': 0}
        batch_size = 1000
        
        for i in range(0, len(qb_records), batch_size):
            batch = qb_records[i:i + batch_size]
            
            payload = {
                'to': table_id,
                'data': batch,
            }
            
            # Use unique key field for merge if available
            if unique_key_field_id:
                payload['mergeFieldId'] = unique_key_field_id
            
            response = requests.post(
                f"{self.base_url}/records",
                headers=self._get_headers(),
                json=payload
            )
            
            if response.status_code == 200:
                metadata = response.json().get('metadata', {})
                results['created'] += metadata.get('createdRecordIds', []).__len__()
                results['updated'] += metadata.get('updatedRecordIds', []).__len__()
            else:
                logger.error(f"Upsert failed for {table_name}: {response.text}")
        
        return results
    
    def upsert_line_items(self, table_name: str, parent_records: List[Dict], 
                          parent_table_name: str, entity_record_id: int, realm_id: str) -> Dict:
        """Extract and upsert line items from parent records (Invoice Lines, Bill Lines, etc.)"""
        
        table_config = QUICKBASE_SCHEMA['tables'].get(table_name)
        if not table_config:
            return {'created': 0, 'updated': 0}
        
        table_id = table_config['table_id']
        fields = table_config['fields']
        
        # Get unique key field from schema
        unique_key_field_id = table_config.get('unique_key_field')
        if not unique_key_field_id:
            unique_key_field_id = self._get_or_create_unique_key_field(table_id, table_name)
        
        # Get parent reference field (links line to parent via QB ID)
        parent_ref_field = table_config.get('parent_ref_field')
        
        # Extract line items
        all_lines = []
        for parent in parent_records:
            parent_id = parent.get('Id')
            parent_unique_key = f"{parent_id}_{realm_id}"
            lines = parent.get('Line', [])
            
            for idx, line in enumerate(lines):
                if not isinstance(line, dict):
                    continue
                
                line_record = {}
                line_num = line.get('LineNum', idx)
                
                # Link to parent via QB_UniqueKey
                if parent_ref_field:
                    line_record[str(parent_ref_field)] = {'value': parent_unique_key}
                
                # Common line fields
                if 'LineNum' in fields:
                    line_record[str(fields['LineNum'])] = {'value': line_num}
                if 'Line Number' in fields:
                    line_record[str(fields['Line Number'])] = {'value': line_num}
                if 'Description' in fields:
                    line_record[str(fields['Description'])] = {'value': line.get('Description')}
                if 'Amount' in fields:
                    line_record[str(fields['Amount'])] = {'value': line.get('Amount')}
                
                # Invoice-specific (SalesItemLineDetail)
                detail = line.get('SalesItemLineDetail', {})
                if detail:
                    if 'Quantity' in fields:
                        line_record[str(fields['Quantity'])] = {'value': detail.get('Qty')}
                    if 'Unit Price' in fields:
                        line_record[str(fields['Unit Price'])] = {'value': detail.get('UnitPrice')}
                    if 'Items' in fields:
                        item_ref = detail.get('ItemRef', {})
                        line_record[str(fields['Items'])] = {'value': item_ref.get('name')}
                
                # Bill line-specific (AccountBasedExpenseLineDetail or ItemBasedExpenseLineDetail)
                acct_detail = line.get('AccountBasedExpenseLineDetail', {})
                item_detail = line.get('ItemBasedExpenseLineDetail', {})
                if acct_detail and 'Account' in fields:
                    acct_ref = acct_detail.get('AccountRef', {})
                    line_record[str(fields['Account'])] = {'value': acct_ref.get('name')}
                
                # JournalEntry-specific (JournalEntryLineDetail)
                je_detail = line.get('JournalEntryLineDetail', {})
                if je_detail:
                    if 'Posting Type' in fields:
                        line_record[str(fields['Posting Type'])] = {'value': je_detail.get('PostingType')}
                    if 'Account' in fields:
                        acct_ref = je_detail.get('AccountRef', {})
                        line_record[str(fields['Account'])] = {'value': acct_ref.get('name')}
                
                # Add composite unique key: {parent_id}_{line_num}_{realm_id}
                if unique_key_field_id:
                    unique_key = f"{parent_id}_{line_num}_{realm_id}"
                    line_record[str(unique_key_field_id)] = {'value': unique_key}
                
                if line_record:
                    all_lines.append(line_record)
        
        if not all_lines:
            return {'created': 0, 'updated': 0}
        
        # Upsert lines
        results = {'created': 0, 'updated': 0}
        batch_size = 1000
        
        for i in range(0, len(all_lines), batch_size):
            batch = all_lines[i:i + batch_size]
            
            payload = {
                'to': table_id,
                'data': batch
            }
            
            # Use unique key field for merge if available
            if unique_key_field_id:
                payload['mergeFieldId'] = unique_key_field_id
            
            response = requests.post(
                f"{self.base_url}/records",
                headers=self._get_headers(),
                json=payload
            )
            
            if response.status_code == 200:
                metadata = response.json().get('metadata', {})
                results['created'] += len(metadata.get('createdRecordIds', []))
                results['updated'] += len(metadata.get('updatedRecordIds', []))
            else:
                logger.error(f"Line item upsert failed for {table_name}: {response.text}")
        
        return results
    
    def _get_nested_value(self, obj: Dict, path: str) -> Any:
        """Get nested value from dict using dot notation (e.g., 'PrimaryEmailAddr.Address')"""
        parts = path.split('.')
        current = obj
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current


# =============================================================================
# Sync Engine
# =============================================================================

class SyncEngine:
    """Orchestrates sync between QuickBooks and QuickBase"""
    
    # Map QB entities to QuickBase table names
    QB_TO_TABLE = {
        'Account': 'Accounts',
        'Customer': 'Customers',
        'Vendor': 'Vendors',
        'Item': 'Items',
        'Invoice': 'Invoices',
        'Bill': 'Bills',
        'Payment': 'Customer Payments',
        'BillPayment': 'Bill Payments',
        'JournalEntry': 'Journal Entries',
        'Purchase': 'Purchases',
        'Deposit': 'Deposits',
        'Transfer': 'Transfers',
        'Class': 'Classes',
        'Department': 'Departments',
    }
    
    # Entities with line items
    LINE_ITEM_TABLES = {
        'Invoice': 'Invoice Lines',
        'Bill': 'Bill Line Items',
        'JournalEntry': 'Journal Entry Lines',
    }
    
    def __init__(self, oauth: QBOAuth, qb_client: QuickBaseClient):
        self.oauth = oauth
        self.qb_client = qb_client
    
    def sync_entity(self, client: QBClient, qb_entity: str, entity_record_id: int, realm_id: str):
        """Sync a single entity type from one QB company to QuickBase"""
        
        table_name = self.QB_TO_TABLE.get(qb_entity)
        if not table_name:
            logger.warning(f"No table mapping for QB entity: {qb_entity}")
            return
        
        # Fetch from QuickBooks
        records = client.get_all_entities(qb_entity)
        if not records:
            return
        
        # Upsert to QuickBase
        results = self.qb_client.upsert_records(table_name, records, entity_record_id, realm_id)
        logger.info(f"  {table_name}: {results['created']} created, {results['updated']} updated")
        
        # Handle line items if applicable
        line_table = self.LINE_ITEM_TABLES.get(qb_entity)
        if line_table:
            line_results = self.qb_client.upsert_line_items(
                line_table, records, table_name, entity_record_id, realm_id
            )
            logger.info(f"  {line_table}: {line_results['created']} created, {line_results['updated']} updated")
    
    def sync_all(self, entities: List[str] = None):
        """Sync all entities from all connected QB companies"""
        
        # Default to all mapped entities
        if entities:
            qb_entities = entities
        else:
            qb_entities = list(self.QB_TO_TABLE.keys())
        
        tokens = self.oauth.token_store.get_all()
        
        if not tokens:
            logger.warning("No QB companies connected. Run with --add-company first.")
            return
        
        logger.info(f"Starting sync for {len(tokens)} companies, {len(qb_entities)} entity types")
        
        for realm_id, token in tokens.items():
            logger.info(f"\n{'='*60}")
            logger.info(f"Syncing: {token.company_name} ({realm_id})")
            logger.info(f"{'='*60}")
            
            # Get or create entity record in QuickBase
            try:
                entity_record_id = self.qb_client.get_or_create_entity(realm_id, token.company_name)
            except Exception as e:
                logger.error(f"Failed to get/create entity record: {e}")
                continue
            
            client = QBClient(token, self.oauth)
            
            for qb_entity in qb_entities:
                try:
                    self.sync_entity(client, qb_entity, entity_record_id, realm_id)
                except Exception as e:
                    logger.error(f"Error syncing {qb_entity}: {e}")
                    continue
            
            # Update sync timestamp
            self.qb_client.update_entity_sync_time(realm_id)
        
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
                expiry = parse_datetime(token.access_token_expiry)
                refresh_expiry = parse_datetime(token.refresh_token_expiry)
                
                if utc_now() < expiry:
                    status = "✓ Valid"
                elif utc_now() < refresh_expiry:
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
