#!/bin/bash
#
# One-time Azure setup for QB Sync Function
#
# Prerequisites:
#   - Azure CLI installed and logged in (az login)
#   - Your QuickBooks and QuickBase credentials ready
#
# Usage:
#   chmod +x setup-azure.sh
#   ./setup-azure.sh
#

set -e

# ============================================================================
# Configuration - EDIT THESE
# ============================================================================

RESOURCE_GROUP="qb-sync-rg"
LOCATION="westus2"
STORAGE_ACCOUNT="qbsyncstorage$(openssl rand -hex 4)"  # Must be globally unique
ACR_NAME="qbsyncacr"
FUNCTION_APP="qb-sync-func"
KEY_VAULT="qb-sync-kv"

# Your credentials (will be stored in Key Vault)
QB_USERNAME="it@dispatchenergy.com"
# QB_PASSWORD will be prompted
QUICKBASE_REALM="dispatchenergy"
# QUICKBASE_TOKEN will be prompted
ACCOUNTS_TABLE_ID=""      # e.g., bxxxxxxxxx
TRANSACTIONS_TABLE_ID=""  # e.g., bxxxxxxxxx

# ============================================================================
# Script
# ============================================================================

echo "=============================================="
echo "QB Sync Azure Setup"
echo "=============================================="

# Prompt for secrets
echo ""
read -sp "Enter QB_PASSWORD: " QB_PASSWORD
echo ""
read -sp "Enter QUICKBASE_TOKEN: " QUICKBASE_TOKEN
echo ""
read -p "Enter ACCOUNTS_TABLE_ID (e.g., bxxxxxxxxx): " ACCOUNTS_TABLE_ID
read -p "Enter TRANSACTIONS_TABLE_ID (e.g., bxxxxxxxxx): " TRANSACTIONS_TABLE_ID

echo ""
echo "Creating Resource Group..."
az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION

echo "Creating Storage Account..."
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS

echo "Creating Container Registry..."
az acr create \
  --name $ACR_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Basic \
  --admin-enabled true

echo "Creating Key Vault..."
az keyvault create \
  --name $KEY_VAULT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION

echo "Storing secrets in Key Vault..."
az keyvault secret set --vault-name $KEY_VAULT --name "QB-USERNAME" --value "$QB_USERNAME"
az keyvault secret set --vault-name $KEY_VAULT --name "QB-PASSWORD" --value "$QB_PASSWORD"
az keyvault secret set --vault-name $KEY_VAULT --name "QUICKBASE-TOKEN" --value "$QUICKBASE_TOKEN"

echo "Creating Function App (Premium plan for container support)..."
az functionapp plan create \
  --name "${FUNCTION_APP}-plan" \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku EP1 \
  --is-linux

# Get ACR credentials
ACR_USERNAME=$(az acr credential show --name $ACR_NAME --query username -o tsv)
ACR_PASSWORD=$(az acr credential show --name $ACR_NAME --query passwords[0].value -o tsv)

echo "Creating Function App..."
az functionapp create \
  --name $FUNCTION_APP \
  --resource-group $RESOURCE_GROUP \
  --plan "${FUNCTION_APP}-plan" \
  --storage-account $STORAGE_ACCOUNT \
  --functions-version 4 \
  --deployment-container-image-name "mcr.microsoft.com/azure-functions/python:4-python3.11" \
  --docker-registry-server-url "https://${ACR_NAME}.azurecr.io" \
  --docker-registry-server-user $ACR_USERNAME \
  --docker-registry-server-password $ACR_PASSWORD

echo "Enabling Managed Identity..."
az functionapp identity assign \
  --name $FUNCTION_APP \
  --resource-group $RESOURCE_GROUP

# Get the identity
IDENTITY=$(az functionapp identity show --name $FUNCTION_APP --resource-group $RESOURCE_GROUP --query principalId -o tsv)

echo "Granting Key Vault access..."
az keyvault set-policy \
  --name $KEY_VAULT \
  --object-id $IDENTITY \
  --secret-permissions get list

echo "Configuring Function App settings..."
KV_URI="https://${KEY_VAULT}.vault.azure.net"

az functionapp config appsettings set \
  --name $FUNCTION_APP \
  --resource-group $RESOURCE_GROUP \
  --settings \
    "QB_USERNAME=@Microsoft.KeyVault(SecretUri=${KV_URI}/secrets/QB-USERNAME/)" \
    "QB_PASSWORD=@Microsoft.KeyVault(SecretUri=${KV_URI}/secrets/QB-PASSWORD/)" \
    "QUICKBASE_TOKEN=@Microsoft.KeyVault(SecretUri=${KV_URI}/secrets/QUICKBASE-TOKEN/)" \
    "QUICKBASE_REALM=${QUICKBASE_REALM}" \
    "ACCOUNTS_TABLE_ID=${ACCOUNTS_TABLE_ID}" \
    "TRANSACTIONS_TABLE_ID=${TRANSACTIONS_TABLE_ID}" \
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE=false"

echo ""
echo "=============================================="
echo "Setup Complete!"
echo "=============================================="
echo ""
echo "Resources created:"
echo "  - Resource Group: $RESOURCE_GROUP"
echo "  - Storage: $STORAGE_ACCOUNT"
echo "  - Container Registry: $ACR_NAME"
echo "  - Key Vault: $KEY_VAULT"
echo "  - Function App: $FUNCTION_APP"
echo ""
echo "Next steps:"
echo ""
echo "1. Create GitHub secret AZURE_CREDENTIALS:"
echo "   az ad sp create-for-rbac --name 'qb-sync-github' \\"
echo "     --role contributor \\"
echo "     --scopes /subscriptions/\$(az account show --query id -o tsv)/resourceGroups/$RESOURCE_GROUP \\"
echo "     --sdk-auth"
echo ""
echo "2. Add the JSON output as a GitHub secret named AZURE_CREDENTIALS"
echo ""
echo "3. Push to main branch to trigger deployment"
echo ""
echo "4. Manual trigger:"
echo "   curl -X POST https://${FUNCTION_APP}.azurewebsites.net/api/sync?code=<function-key>"
echo ""
