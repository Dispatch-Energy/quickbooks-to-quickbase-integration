#!/bin/bash
# =============================================================================
# Azure Container Apps Deployment - QB Scraper with Auto-Login
# =============================================================================
#
# Architecture:
#   Container App Job (scheduled) runs Playwright login + HTTP scrape
#   - Login via Playwright (~30s)
#   - Scrape via HTTP API (~10s)  
#   - Sync to QuickBase (~5s)
#   - Total runtime: ~1 minute
#
# Credentials stored in Azure Key Vault for security.
#
# Usage:
#   ./deploy.sh setup           # Create all resources
#   ./deploy.sh deploy          # Build and deploy container
#   ./deploy.sh run             # Trigger manual run
#   ./deploy.sh logs            # View logs
#
# =============================================================================

set -e

# Configuration
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-rg-qb-scraper}"
LOCATION="${AZURE_LOCATION:-westus2}"
CONTAINER_REGISTRY="${AZURE_CONTAINER_REGISTRY:-crqbscraper}"
CONTAINER_APP_ENV="${AZURE_CONTAINER_APP_ENV:-cae-qb-scraper}"
CONTAINER_APP_JOB="${AZURE_CONTAINER_APP_JOB:-qb-scraper-job}"
KEY_VAULT="${AZURE_KEY_VAULT:-kv-qb-scraper}"
IMAGE_NAME="qb-bank-scraper"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# Schedule (cron) - Default: Daily at 6 AM UTC
SCHEDULE_CRON="${SCHEDULE_CRON:-0 6 * * *}"

# QuickBase config
QUICKBASE_REALM="${QUICKBASE_REALM:-}"
QUICKBASE_TOKEN="${QUICKBASE_TOKEN:-}"
QUICKBASE_TABLE_ID="${QUICKBASE_TABLE_ID:-}"

# QB credentials (will be stored in Key Vault)
QB_USERNAME="${QB_USERNAME:-}"
QB_PASSWORD="${QB_PASSWORD:-}"

# Alert webhook (optional)
ALERT_WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"

log() { echo "[$(date '+%H:%M:%S')] $1"; }
error() { echo "[ERROR] $1" >&2; exit 1; }

check_az() {
    az account show &>/dev/null || error "Not logged in. Run 'az login'"
}

load_env() {
    [ -f .env ] && source .env
}

# =============================================================================
# Setup
# =============================================================================

cmd_setup() {
    log "Setting up Azure resources..."
    check_az
    load_env
    
    # Resource group
    log "Creating resource group: $RESOURCE_GROUP"
    az group create -n "$RESOURCE_GROUP" -l "$LOCATION" --tags project=qb-scraper
    
    # Container Registry
    log "Creating Container Registry: $CONTAINER_REGISTRY"
    az acr create \
        -n "$CONTAINER_REGISTRY" \
        -g "$RESOURCE_GROUP" \
        --sku Basic \
        --admin-enabled true
    
    # Key Vault for secrets
    log "Creating Key Vault: $KEY_VAULT"
    az keyvault create \
        -n "$KEY_VAULT" \
        -g "$RESOURCE_GROUP" \
        -l "$LOCATION" \
        --enable-rbac-authorization false
    
    # Store secrets in Key Vault
    log "Storing secrets in Key Vault..."
    
    if [ -n "$QB_USERNAME" ]; then
        az keyvault secret set --vault-name "$KEY_VAULT" -n "qb-username" --value "$QB_USERNAME"
        log "  ✓ QB_USERNAME stored"
    else
        log "  ⚠ QB_USERNAME not set - add later with: az keyvault secret set --vault-name $KEY_VAULT -n qb-username --value 'your-email'"
    fi
    
    if [ -n "$QB_PASSWORD" ]; then
        az keyvault secret set --vault-name "$KEY_VAULT" -n "qb-password" --value "$QB_PASSWORD"
        log "  ✓ QB_PASSWORD stored"
    else
        log "  ⚠ QB_PASSWORD not set - add later with: az keyvault secret set --vault-name $KEY_VAULT -n qb-password --value 'your-password'"
    fi
    
    if [ -n "$QUICKBASE_TOKEN" ]; then
        az keyvault secret set --vault-name "$KEY_VAULT" -n "quickbase-token" --value "$QUICKBASE_TOKEN"
        log "  ✓ QUICKBASE_TOKEN stored"
    fi
    
    # Container Apps Environment
    log "Creating Container Apps Environment: $CONTAINER_APP_ENV"
    az containerapp env create \
        -n "$CONTAINER_APP_ENV" \
        -g "$RESOURCE_GROUP" \
        -l "$LOCATION"
    
    log ""
    log "Setup complete!"
    log ""
    log "Next steps:"
    log "  1. Store credentials:     ./deploy.sh set-credentials"
    log "  2. Build container:       ./deploy.sh build"
    log "  3. Deploy job:            ./deploy.sh deploy"
    log "  4. Test:                  ./deploy.sh run"
}

cmd_set_credentials() {
    log "Setting credentials in Key Vault..."
    check_az
    
    read -p "QuickBooks Username (email): " qb_user
    read -s -p "QuickBooks Password: " qb_pass
    echo ""
    
    az keyvault secret set --vault-name "$KEY_VAULT" -n "qb-username" --value "$qb_user"
    az keyvault secret set --vault-name "$KEY_VAULT" -n "qb-password" --value "$qb_pass"
    
    log "Credentials stored in Key Vault"
}

# =============================================================================
# Build
# =============================================================================

cmd_build() {
    log "Building container image..."
    check_az
    
    ACR_LOGIN_SERVER=$(az acr show -n "$CONTAINER_REGISTRY" -g "$RESOURCE_GROUP" --query loginServer -o tsv)
    FULL_IMAGE="$ACR_LOGIN_SERVER/$IMAGE_NAME:$IMAGE_TAG"
    
    az acr login -n "$CONTAINER_REGISTRY"
    
    docker build -t "$FULL_IMAGE" -f Dockerfile.playwright .
    docker push "$FULL_IMAGE"
    
    log "Image pushed: $FULL_IMAGE"
}

# =============================================================================
# Deploy
# =============================================================================

cmd_deploy() {
    log "Deploying Container App Job..."
    check_az
    load_env
    
    ACR_LOGIN_SERVER=$(az acr show -n "$CONTAINER_REGISTRY" -g "$RESOURCE_GROUP" --query loginServer -o tsv)
    ACR_USERNAME=$(az acr credential show -n "$CONTAINER_REGISTRY" -g "$RESOURCE_GROUP" --query username -o tsv)
    ACR_PASSWORD=$(az acr credential show -n "$CONTAINER_REGISTRY" -g "$RESOURCE_GROUP" --query 'passwords[0].value' -o tsv)
    
    FULL_IMAGE="$ACR_LOGIN_SERVER/$IMAGE_NAME:$IMAGE_TAG"
    
    # Get secrets from Key Vault
    QB_USER=$(az keyvault secret show --vault-name "$KEY_VAULT" -n "qb-username" --query value -o tsv 2>/dev/null || echo "")
    QB_PASS=$(az keyvault secret show --vault-name "$KEY_VAULT" -n "qb-password" --query value -o tsv 2>/dev/null || echo "")
    QB_TOKEN=$(az keyvault secret show --vault-name "$KEY_VAULT" -n "quickbase-token" --query value -o tsv 2>/dev/null || echo "$QUICKBASE_TOKEN")
    
    # Check if job exists
    if az containerapp job show -n "$CONTAINER_APP_JOB" -g "$RESOURCE_GROUP" &>/dev/null; then
        log "Updating existing job..."
        az containerapp job update \
            -n "$CONTAINER_APP_JOB" \
            -g "$RESOURCE_GROUP" \
            --image "$FULL_IMAGE"
    else
        log "Creating new job..."
        az containerapp job create \
            -n "$CONTAINER_APP_JOB" \
            -g "$RESOURCE_GROUP" \
            --environment "$CONTAINER_APP_ENV" \
            --trigger-type "Schedule" \
            --cron-expression "$SCHEDULE_CRON" \
            --replica-timeout 600 \
            --replica-retry-limit 1 \
            --parallelism 1 \
            --replica-completion-count 1 \
            --image "$FULL_IMAGE" \
            --cpu 1 \
            --memory "2Gi" \
            --registry-server "$ACR_LOGIN_SERVER" \
            --registry-username "$ACR_USERNAME" \
            --registry-password "$ACR_PASSWORD" \
            --env-vars \
                "QB_USERNAME=$QB_USER" \
                "QB_PASSWORD=$QB_PASS" \
                "QUICKBASE_REALM=${QUICKBASE_REALM}" \
                "QUICKBASE_TOKEN=$QB_TOKEN" \
                "QUICKBASE_TABLE_ID=${QUICKBASE_TABLE_ID}" \
                "ALERT_WEBHOOK_URL=${ALERT_WEBHOOK_URL}"
    fi
    
    log "Deployment complete! Schedule: $SCHEDULE_CRON"
}

# =============================================================================
# Operations
# =============================================================================

cmd_run() {
    log "Triggering manual job execution..."
    check_az
    
    az containerapp job start -n "$CONTAINER_APP_JOB" -g "$RESOURCE_GROUP"
    log "Job triggered. View logs with: ./deploy.sh logs"
}

cmd_logs() {
    log "Fetching recent logs..."
    check_az
    
    EXECUTION=$(az containerapp job execution list -n "$CONTAINER_APP_JOB" -g "$RESOURCE_GROUP" --query '[0].name' -o tsv)
    
    if [ -n "$EXECUTION" ]; then
        log "Latest execution: $EXECUTION"
        az containerapp job logs show \
            -n "$CONTAINER_APP_JOB" \
            -g "$RESOURCE_GROUP" \
            --execution "$EXECUTION" \
            --follow
    else
        log "No executions found"
    fi
}

cmd_status() {
    log "Job status..."
    check_az
    
    az containerapp job execution list \
        -n "$CONTAINER_APP_JOB" \
        -g "$RESOURCE_GROUP" \
        --output table
}

cmd_destroy() {
    log "WARNING: This will delete all resources including Key Vault secrets!"
    read -p "Type 'yes' to confirm: " confirm
    [ "$confirm" = "yes" ] || { log "Aborted."; exit 0; }
    
    check_az
    az group delete -n "$RESOURCE_GROUP" --yes --no-wait
    log "Deletion initiated"
}

# =============================================================================
# Main
# =============================================================================

load_env

case "${1:-help}" in
    setup)           cmd_setup ;;
    set-credentials) cmd_set_credentials ;;
    build)           cmd_build ;;
    deploy)          cmd_deploy ;;
    run)             cmd_run ;;
    logs)            cmd_logs ;;
    status)          cmd_status ;;
    destroy)         cmd_destroy ;;
    *)
        echo "Azure Container Apps - QB Scraper with Auto-Login"
        echo ""
        echo "Usage: $0 <command>"
        echo ""
        echo "Setup:"
        echo "  setup           - Create Azure resources"
        echo "  set-credentials - Store QB credentials in Key Vault"
        echo "  build           - Build and push container"
        echo "  deploy          - Deploy job"
        echo ""
        echo "Operations:"
        echo "  run             - Trigger manual run"
        echo "  logs            - View logs"
        echo "  status          - Show executions"
        echo "  destroy         - Delete all resources"
        echo ""
        ;;
esac
