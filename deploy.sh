#!/bin/bash
# =============================================================================
# Azure Functions Deployment for QB Bank Scraper
# =============================================================================
#
# Prerequisites:
#   - Azure CLI (az) installed and logged in
#   - Azure Functions Core Tools (func) installed
#   - Session file created locally via Playwright login
#
# Usage:
#   ./deploy.sh setup           # Create Azure resources
#   ./deploy.sh deploy          # Deploy function code
#   ./deploy.sh upload-session  # Upload session to blob storage
#   ./deploy.sh test            # Trigger manual scrape
#   ./deploy.sh logs            # View recent logs
#   ./deploy.sh destroy         # Delete all resources
#
# =============================================================================

set -e

# Configuration
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-rg-qb-scraper}"
LOCATION="${AZURE_LOCATION:-westus2}"
STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-stqbscraper$(date +%s | tail -c 6)}"
FUNCTION_APP="${AZURE_FUNCTION_APP:-func-qb-scraper}"
APP_INSIGHTS="${AZURE_APP_INSIGHTS:-appi-qb-scraper}"

# Blob container for session
SESSION_CONTAINER="qb-session"
SESSION_BLOB="state.json"

# QuickBase config (from environment or .env)
source .env 2>/dev/null || true

log() { echo "[$(date '+%H:%M:%S')] $1"; }
error() { echo "[ERROR] $1" >&2; exit 1; }

check_az() {
    az account show &>/dev/null || error "Not logged in. Run 'az login'"
}

# =============================================================================
# Commands
# =============================================================================

cmd_setup() {
    log "Creating Azure resources..."
    check_az
    
    # Resource group
    log "Creating resource group: $RESOURCE_GROUP"
    az group create -n "$RESOURCE_GROUP" -l "$LOCATION" --tags project=qb-scraper
    
    # Storage account
    log "Creating storage account: $STORAGE_ACCOUNT"
    az storage account create \
        -n "$STORAGE_ACCOUNT" \
        -g "$RESOURCE_GROUP" \
        -l "$LOCATION" \
        --sku Standard_LRS \
        --kind StorageV2
    
    # Get connection string
    CONN_STR=$(az storage account show-connection-string \
        -n "$STORAGE_ACCOUNT" \
        -g "$RESOURCE_GROUP" \
        -o tsv)
    
    # Create blob container for session
    log "Creating session container: $SESSION_CONTAINER"
    az storage container create \
        -n "$SESSION_CONTAINER" \
        --connection-string "$CONN_STR" \
        --public-access off
    
    # Application Insights
    log "Creating Application Insights: $APP_INSIGHTS"
    az monitor app-insights component create \
        -a "$APP_INSIGHTS" \
        -g "$RESOURCE_GROUP" \
        -l "$LOCATION" \
        --application-type web
    
    INSIGHTS_KEY=$(az monitor app-insights component show \
        -a "$APP_INSIGHTS" \
        -g "$RESOURCE_GROUP" \
        --query instrumentationKey -o tsv)
    
    # Function App (Consumption plan)
    log "Creating Function App: $FUNCTION_APP"
    az functionapp create \
        -n "$FUNCTION_APP" \
        -g "$RESOURCE_GROUP" \
        --storage-account "$STORAGE_ACCOUNT" \
        --consumption-plan-location "$LOCATION" \
        --runtime python \
        --runtime-version 3.11 \
        --functions-version 4 \
        --os-type Linux \
        --app-insights "$APP_INSIGHTS" \
        --app-insights-key "$INSIGHTS_KEY"
    
    # Configure app settings
    log "Configuring app settings..."
    az functionapp config appsettings set \
        -n "$FUNCTION_APP" \
        -g "$RESOURCE_GROUP" \
        --settings \
            "QUICKBASE_REALM=${QUICKBASE_REALM:-}" \
            "QUICKBASE_TOKEN=${QUICKBASE_TOKEN:-}" \
            "QUICKBASE_TABLE_ID=${QUICKBASE_TABLE_ID:-}" \
            "ALERT_WEBHOOK_URL=${ALERT_WEBHOOK_URL:-}"
    
    log ""
    log "Setup complete!"
    log ""
    log "Storage Account: $STORAGE_ACCOUNT"
    log "Function App:    $FUNCTION_APP"
    log ""
    log "Next steps:"
    log "  1. Create session locally: python qb_http_scraper.py --login"
    log "  2. Upload session:         ./deploy.sh upload-session"
    log "  3. Deploy function:        ./deploy.sh deploy"
    log "  4. Test:                   ./deploy.sh test"
}

cmd_deploy() {
    log "Deploying function code..."
    check_az
    
    # Deploy using func CLI
    func azure functionapp publish "$FUNCTION_APP" --python
    
    log "Deployment complete!"
    log "Function URL: https://${FUNCTION_APP}.azurewebsites.net/api/"
}

cmd_upload_session() {
    log "Uploading session to blob storage..."
    check_az
    
    LOCAL_SESSION="${HOME}/.qb_bank_scraper/session/state.json"
    
    if [ ! -f "$LOCAL_SESSION" ]; then
        error "Session file not found: $LOCAL_SESSION"
        error "Run: python qb_http_scraper.py --login"
    fi
    
    # Get connection string
    CONN_STR=$(az storage account show-connection-string \
        -n "$STORAGE_ACCOUNT" \
        -g "$RESOURCE_GROUP" \
        -o tsv)
    
    # Upload
    az storage blob upload \
        --file "$LOCAL_SESSION" \
        --container-name "$SESSION_CONTAINER" \
        --name "$SESSION_BLOB" \
        --connection-string "$CONN_STR" \
        --overwrite
    
    log "Session uploaded successfully!"
}

cmd_download_session() {
    log "Downloading session from blob storage..."
    check_az
    
    LOCAL_DIR="${HOME}/.qb_bank_scraper/session"
    mkdir -p "$LOCAL_DIR"
    
    CONN_STR=$(az storage account show-connection-string \
        -n "$STORAGE_ACCOUNT" \
        -g "$RESOURCE_GROUP" \
        -o tsv)
    
    az storage blob download \
        --container-name "$SESSION_CONTAINER" \
        --name "$SESSION_BLOB" \
        --file "$LOCAL_DIR/state.json" \
        --connection-string "$CONN_STR"
    
    log "Session downloaded to: $LOCAL_DIR/state.json"
}

cmd_test() {
    log "Triggering manual scrape..."
    check_az
    
    # Get function key
    KEY=$(az functionapp keys list \
        -n "$FUNCTION_APP" \
        -g "$RESOURCE_GROUP" \
        --query 'functionKeys.default' -o tsv)
    
    # Call the scrape endpoint
    URL="https://${FUNCTION_APP}.azurewebsites.net/api/scrape?code=${KEY}"
    
    log "Calling: $URL"
    curl -X POST "$URL" -H "Content-Type: application/json"
    echo ""
}

cmd_check_session() {
    log "Checking session validity..."
    check_az
    
    KEY=$(az functionapp keys list \
        -n "$FUNCTION_APP" \
        -g "$RESOURCE_GROUP" \
        --query 'functionKeys.default' -o tsv)
    
    URL="https://${FUNCTION_APP}.azurewebsites.net/api/check-session?code=${KEY}"
    
    curl -s "$URL" | python -m json.tool
}

cmd_logs() {
    log "Streaming function logs..."
    check_az
    
    az functionapp log tail \
        -n "$FUNCTION_APP" \
        -g "$RESOURCE_GROUP"
}

cmd_status() {
    log "Function App status..."
    check_az
    
    az functionapp show \
        -n "$FUNCTION_APP" \
        -g "$RESOURCE_GROUP" \
        --query '{name:name, state:state, url:defaultHostName}' \
        -o table
    
    echo ""
    log "Recent invocations (from App Insights):"
    az monitor app-insights query \
        -a "$APP_INSIGHTS" \
        -g "$RESOURCE_GROUP" \
        --analytics-query "requests | where timestamp > ago(24h) | project timestamp, name, success, duration | order by timestamp desc | take 10" \
        -o table 2>/dev/null || log "(App Insights query requires additional setup)"
}

cmd_set_schedule() {
    # Note: Timer trigger schedule is defined in code (function_app.py)
    # This is just a helper to update via az CLI
    log "Schedule is defined in function_app.py"
    log "Current setting: 0 0 6 * * * (6 AM UTC daily)"
    log ""
    log "To change, edit the @app.schedule decorator in function_app.py:"
    log '  @app.schedule(schedule="0 0 */4 * * *", ...)  # Every 4 hours'
    log '  @app.schedule(schedule="0 0 * * * *", ...)    # Hourly'
    log ""
    log "Then redeploy: ./deploy.sh deploy"
}

cmd_destroy() {
    log "WARNING: This will delete all resources!"
    read -p "Type 'yes' to confirm: " confirm
    
    [ "$confirm" = "yes" ] || { log "Aborted."; exit 0; }
    
    check_az
    
    az group delete -n "$RESOURCE_GROUP" --yes --no-wait
    log "Deletion initiated"
}

# =============================================================================
# Main
# =============================================================================

case "${1:-help}" in
    setup)           cmd_setup ;;
    deploy)          cmd_deploy ;;
    upload-session)  cmd_upload_session ;;
    download-session) cmd_download_session ;;
    test)            cmd_test ;;
    check-session)   cmd_check_session ;;
    logs)            cmd_logs ;;
    status)          cmd_status ;;
    set-schedule)    cmd_set_schedule ;;
    destroy)         cmd_destroy ;;
    *)
        echo "Azure Functions Deployment for QB Scraper"
        echo ""
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  setup           - Create Azure resources"
        echo "  deploy          - Deploy function code"
        echo "  upload-session  - Upload local session to Azure"
        echo "  download-session - Download session from Azure"
        echo "  test            - Trigger manual scrape"
        echo "  check-session   - Check if session is valid"
        echo "  logs            - Stream function logs"
        echo "  status          - Show function status"
        echo "  set-schedule    - Info about changing schedule"
        echo "  destroy         - Delete all resources"
        echo ""
        ;;
esac
