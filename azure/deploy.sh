#!/usr/bin/env bash
# Deploy Strategic Task Management to Azure Container Apps.
#
# Usage:
#   ./azure/deploy.sh [--resource-group RG] [--env-name NAME] [--registry REG]
#
# Prerequisites:
#   - az CLI logged in (az login)
#   - Docker logged in to the registry (az acr login --name REGISTRY)
#   - A parameters file at azure/parameters.json (copy from parameters.example.json)
#
# What this script does:
#   1. Builds and pushes both images to the container registry
#   2. Runs the Bicep deployment
#   3. Prints the app URL
#
# Secrets must live in Azure Key Vault and be referenced in parameters.json.
# Do NOT pass secrets as plain CLI arguments or store them in parameters.json.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ---------- Parse arguments ---------------------------------------------------
RESOURCE_GROUP="${RESOURCE_GROUP:-}"
ENV_NAME="${ENV_NAME:-stm-prod}"
REGISTRY="${REGISTRY:-}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
PARAMETERS_FILE="${SCRIPT_DIR}/parameters.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --resource-group|-g) RESOURCE_GROUP="$2"; shift 2 ;;
    --env-name|-e)       ENV_NAME="$2";       shift 2 ;;
    --registry|-r)       REGISTRY="$2";       shift 2 ;;
    --tag|-t)            IMAGE_TAG="$2";       shift 2 ;;
    --parameters|-p)     PARAMETERS_FILE="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ -z "${RESOURCE_GROUP}" ]]; then
  echo "ERROR: --resource-group is required"
  exit 1
fi
if [[ -z "${REGISTRY}" ]]; then
  echo "ERROR: --registry is required (e.g. myregistry.azurecr.io)"
  exit 1
fi
if [[ ! -f "${PARAMETERS_FILE}" ]]; then
  echo "ERROR: parameters file not found: ${PARAMETERS_FILE}"
  echo "       Copy azure/parameters.example.json to azure/parameters.json and fill in your values."
  exit 1
fi

APP_IMAGE="${REGISTRY}/stm-app:${IMAGE_TAG}"
SYNC_IMAGE="${REGISTRY}/stm-sync:${IMAGE_TAG}"

echo "==> Building images"
docker build --target app  -t "${APP_IMAGE}"  "${REPO_ROOT}"
docker build --target sync -t "${SYNC_IMAGE}" "${REPO_ROOT}"

echo "==> Pushing images to ${REGISTRY}"
docker push "${APP_IMAGE}"
docker push "${SYNC_IMAGE}"

echo "==> Deploying Bicep template"
az deployment group create \
  --resource-group "${RESOURCE_GROUP}" \
  --template-file "${SCRIPT_DIR}/container-apps.bicep" \
  --parameters "@${PARAMETERS_FILE}" \
  --parameters appImageTag="${APP_IMAGE}" syncImageTag="${SYNC_IMAGE}" \
  --output table

echo ""
FQDN=$(az deployment group show \
  --resource-group "${RESOURCE_GROUP}" \
  --name container-apps \
  --query properties.outputs.appFqdn.value \
  --output tsv 2>/dev/null || echo "(run 'az deployment group show' to retrieve the URL)")

echo "==> Deployment complete"
echo "    App URL: https://${FQDN}"
echo ""
echo "    Next steps:"
echo "    1. Enable Azure App Service Authentication (Easy Auth) on the app container."
echo "    2. Set AUTH_MODE=app_service and APP_TRUSTED_PROXY_SECRET in application settings."
echo "    3. Grant the sync managed identity read access to your SharePoint/Graph sources."
