#! /bin/bash
# This script is used to deploy new versions of the eth-analysis-rs backend services

set -e

CLUSTER=$1
SERVICE_NAME=$2

# --- Configuration ---
ETH_ANALYSIS_RS_DIR="/Users/alextes/code/ultra-sound/eth-analysis-rs"
WEB_INFRA_DIR="/Users/alextes/code/ultra-sound/web-infra"
IMAGE_BASE_NAME="rg.fr-par.scw.cloud/ultrasoundmoney/eth-analysis-rs"
VALID_SERVICES=("sync-beacon-states" "sync-execution-blocks" "sync-execution-supply-deltas" "geth-supply-live") # Add other backend services as needed
# --- End Configuration ---

# Validate cluster
if [[ "$CLUSTER" == "stag" ]]; then
    TARGET_CLUSTER="smith"
elif [[ "$CLUSTER" == "prod" ]]; then
    TARGET_CLUSTER="kevin"
else
    echo "Unknown cluster: $CLUSTER"
    echo "Usage: ./deploy-backend.sh [stag|prod] [service-name]"
    exit 1
fi

# Validate service name
if [[ ! " ${VALID_SERVICES[@]} " =~ " ${SERVICE_NAME} " ]]; then
    echo "Unknown service name: $SERVICE_NAME"
    echo "Valid services are: ${VALID_SERVICES[*]}"
    echo "Usage: ./deploy-backend.sh [stag|prod] [service-name]"
    exit 1
fi

# Navigate to the backend project directory
cd "$ETH_ANALYSIS_RS_DIR"

# Check for dirty Git repository
if [ -n "$(git status --untracked-files=no --porcelain)" ]; then
    echo "eth-analysis-rs repo is dirty, please commit changes before deploying"
    exit 1
fi

CURRENT_COMMIT_SHORT=$(git rev-parse --short=7 HEAD)
CURRENT_COMMIT_FULL=$(git rev-parse HEAD) # For CI status, as it often uses full SHA

echo "-> Deploying $SERVICE_NAME (backend) - $CURRENT_COMMIT_SHORT to $TARGET_CLUSTER"
echo "-> Pushing latest code to GitHub"
git push

echo "-> Checking for CI success for commit $CURRENT_COMMIT_FULL"
echo "   (This script uses 'hub' CLI for CI status. Ensure it's installed and configured.)"
sleep 6 # Give CI some time to start

# hub ci-status might exit the script if it fails, wrap in subshell to control exit
CI_CHECK_OUTPUT=$( (
    set +e # Allow hub to fail without exiting the main script immediately
    ci_status_val=""
    retries=0
    max_retries=10 # Approx 160 seconds + initial 6s wait

    while [[ $retries -lt $max_retries ]]; do
        ci_status_val=$(hub ci-status "$CURRENT_COMMIT_FULL" 2>&1) # Capture stderr too
        if [[ $ci_status_val == "pending" ]]; then
            echo "-> CI status: pending, trying again in 16 seconds (attempt $((retries + 1))/$max_retries)"
            sleep 16
            retries=$((retries + 1))
        elif [[ $ci_status_val == "success" || $ci_status_val == "failure" || $ci_status_val == "error" || $ci_status_val == "no status" ]]; then
            break
        else
            # Handle unexpected output from hub ci-status, could be an error message
            echo "-> Unexpected CI status output: $ci_status_val. Assuming 'no status' and will ask to continue."
            ci_status_val="no status" # Treat as no status
            break
        fi
    done

    if [[ $retries -eq $max_retries && $ci_status_val == "pending" ]]; then
        echo "-> CI status still pending after multiple retries."
        ci_status_val="no status" # Treat as no status if timeout
    fi
    
    echo "$ci_status_val" # Pass the final status out of the subshell
) )

CI_STATUS="$CI_CHECK_OUTPUT"

if [[ "$CI_STATUS" == "failure" || "$CI_STATUS" == "error" ]]; then
    echo "-> CI failure or error detected for commit $CURRENT_COMMIT_SHORT ($CI_STATUS)"
    exit 1
elif [[ "$CI_STATUS" == "no status" || "$CI_STATUS" == "" ]]; then # Check for empty string too
    read -p "-> No CI status available (or timed out) for commit $CURRENT_COMMIT_SHORT. Status: '$CI_STATUS'. Continue deployment? (y/n): " CONTINUE
    if [[ "$CONTINUE" != "y" ]]; then
        echo "-> Aborting deployment."
        exit 1
    fi
    echo "-> Continuing deployment despite missing CI status."
elif [[ "$CI_STATUS" == "success" ]]; then
    echo "-> CI success for commit $CURRENT_COMMIT_SHORT."
else # Should not happen if logic above is correct
    echo "-> Unknown CI status: '$CI_STATUS'. Aborting."
    exit 1
fi

echo "-> Continuing with deploy of $SERVICE_NAME."

# Update Kubernetes Deployment
echo "-> Switching kubectl context to $TARGET_CLUSTER"
kubectl config use-context "$TARGET_CLUSTER"

KUBE_DEPLOYMENT_NAME="$SERVICE_NAME" # Assuming k8s deployment name matches service name
# Assuming container name inside deployment matches service name. Adjust if different.
KUBE_CONTAINER_NAME="$SERVICE_NAME" 
FULL_IMAGE_TAG="$IMAGE_BASE_NAME:$CURRENT_COMMIT_SHORT"

echo "-> Updating deployment/$KUBE_DEPLOYMENT_NAME, container $KUBE_CONTAINER_NAME to image $FULL_IMAGE_TAG"
kubectl set image "deployment/$KUBE_DEPLOYMENT_NAME" "$KUBE_CONTAINER_NAME=$FULL_IMAGE_TAG" --record

# Update web-infra YAML only when deploying to staging
if [[ "$TARGET_CLUSTER" == "smith" ]]; then
    echo "-> Updating web-infra YAML for staging deployment."
    cd "$WEB_INFRA_DIR"

    # Check for dirty Git repository in web-infra
    if [ -n "$(git status --untracked-files=no --porcelain)" ]; then
        echo "-> web-infra repo is dirty, please commit changes before deploying"
        exit 1
    fi

    # Ensure we are on the main branch
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$CURRENT_BRANCH" != "main" ]]; then
        echo "-> web-infra repo is not on main branch. Attempting to switch to main..."
        git checkout main
        if [ $? -ne 0 ]; then
            echo "-> Failed to switch to main branch in web-infra. Please resolve any issues and try again."
            exit 1
        fi
        echo "-> Switched web-infra to main branch."
    fi

    echo "-> Pulling latest web-infra changes from GitHub"
    git pull --quiet

    DEPLOY_FILE_PATH="$WEB_INFRA_DIR/base/$SERVICE_NAME.yaml"
    if [ ! -f "$DEPLOY_FILE_PATH" ]; then
        echo "-> ERROR: Deployment YAML file not found at $DEPLOY_FILE_PATH"
        exit 1
    fi
    
    echo "-> Updating $DEPLOY_FILE_PATH image to $CURRENT_COMMIT_SHORT"
    # Note: The sed command uses | as a delimiter to avoid issues with paths/URLs in IMAGE_BASE_NAME
    sed -i.bak "s|$IMAGE_BASE_NAME:[a-f0-9]\{7,40\}|$FULL_IMAGE_TAG|" "$DEPLOY_FILE_PATH"
    rm -f "${DEPLOY_FILE_PATH}.bak" # Remove backup file created by sed -i on macOS

    echo "-> Creating commit in web-infra"
    git add "$DEPLOY_FILE_PATH"
    git commit -m "chore(eth-analysis-rs): update $SERVICE_NAME image to $CURRENT_COMMIT_SHORT"
    
    echo "-> Pushing web-infra changes to GitHub"
    git push --quiet
else
    echo "-> Skipping web-infra YAML update (not deploying to stag/smith)."
fi

echo "-> Deployment of $SERVICE_NAME to $TARGET_CLUSTER initiated."
osascript -e "display notification \"Deployed $SERVICE_NAME backend to $TARGET_CLUSTER ($CURRENT_COMMIT_SHORT)\" with title \"Deployment Succeeded\""

echo "-> Done." 