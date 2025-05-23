#! /bin/bash
# This script is used to deploy new versions of the eth-analysis-rs backend services

set -e

CLUSTER=$1
SERVICE_NAME=$2
# Added optional force deploy flag (third argument)
FORCE_DEPLOY=false
if [[ "$3" == "--force" || "$3" == "-f" ]]; then
    FORCE_DEPLOY=true
fi
TARGET_ENV_NAME="" # Will be dev, stag, or prod

# --- Configuration ---
ETH_ANALYSIS_RS_DIR="/Users/alextes/code/ultra-sound/eth-analysis-rs"
WEB_INFRA_DIR="/Users/alextes/code/ultra-sound/web-infra"
IMAGE_BASE_NAME="ultrasoundorg/eth-analysis-rs"
VALID_SERVICES=("sync-beacon-states" "sync-execution-blocks" "sync-execution-supply-deltas" "geth-supply-live") # Add other backend services as needed
# --- End Configuration ---

# Validate cluster
if [[ "$CLUSTER" == "dev" ]]; then
    TARGET_CLUSTER="smith"
    TARGET_ENV_NAME="dev"
elif [[ "$CLUSTER" == "stag" ]]; then
    TARGET_CLUSTER="smith"
    TARGET_ENV_NAME="staging"
elif [[ "$CLUSTER" == "prod" ]]; then
    TARGET_CLUSTER="kevin"
    TARGET_ENV_NAME="production"
else
    echo "error: unknown cluster: $CLUSTER"
    echo "usage: ./deploy-backend.sh [dev|stag|prod] [service-name] [--force]"
    exit 1
fi

# Validate service name
if [[ ! " ${VALID_SERVICES[@]} " =~ " ${SERVICE_NAME} " ]]; then
    echo "error: unknown service name: $SERVICE_NAME"
    echo "valid services are: ${VALID_SERVICES[*]}"
    echo "usage: ./deploy-backend.sh [dev|stag|prod] [service-name] [--force]"
    exit 1
fi

# Navigate to the backend project directory
cd "$ETH_ANALYSIS_RS_DIR"

# Check for dirty Git repository
if [ -n "$(git status --untracked-files=no --porcelain)" ]; then
    echo "error: eth-analysis-rs repo is dirty, please commit changes before deploying"
    exit 1
fi

CURRENT_COMMIT_SHORT=$(git rev-parse --short=7 HEAD)
CURRENT_COMMIT_FULL=$(git rev-parse HEAD) # For CI status, as it often uses full SHA

echo "-> deploying $SERVICE_NAME (backend) - $CURRENT_COMMIT_SHORT to $CLUSTER ($TARGET_CLUSTER)"
echo "-> pushing latest code to github"
git push

if [[ "$FORCE_DEPLOY" == true ]]; then
    echo "-> force deploy enabled; skipping ci checks entirely."
    CI_STATUS="skipped"
else
    echo "-> checking for ci success for commit $CURRENT_COMMIT_FULL"
    # Check if hub is installed
    if ! command -v hub &> /dev/null; then
        echo "error: hub cli is not installed. please install it to continue."
        echo "       (see: https://hub.github.com/)"
        exit 1
    fi
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
                echo "-> ci status: pending, trying again in 16 seconds (attempt $((retries + 1))/$max_retries)"
                sleep 16
                retries=$((retries + 1))
            elif [[ $ci_status_val == "success" || $ci_status_val == "failure" || $ci_status_val == "error" || $ci_status_val == "no status" ]]; then
                break
            else
                # Handle unexpected output from hub ci-status, could be an error message
                echo "-> unexpected ci status output: $ci_status_val. assuming 'no status' and will ask to continue."
                ci_status_val="no status" # Treat as no status
                break
            fi
        done

        if [[ $retries -eq $max_retries && $ci_status_val == "pending" ]]; then
            echo "-> ci status still pending after multiple retries."
            ci_status_val="no status" # Treat as no status if timeout
        fi
        
        echo "$ci_status_val" # Pass the final status out of the subshell
    ) )

    CI_STATUS="$CI_CHECK_OUTPUT"
fi

# If force deploy flag is set, skip CI status enforcement
if [[ "$FORCE_DEPLOY" == true ]]; then
    echo "-> force deploy enabled. skipping ci status enforcement (ci status: $CI_STATUS)."
else
    if [[ "$CI_STATUS" == "failure" || "$CI_STATUS" == "error" ]]; then
        echo "-> ci failure or error detected for commit $CURRENT_COMMIT_SHORT ($CI_STATUS)"
        exit 1
    elif [[ "$CI_STATUS" == "no status" || "$CI_STATUS" == "" ]]; then # Check for empty string too
        read -p "-> no ci status available (or timed out) for commit $CURRENT_COMMIT_SHORT. status: '$CI_STATUS'. continue deployment? (y/n): " CONTINUE
        if [[ "$CONTINUE" != "y" ]]; then
            echo "-> aborting deployment."
            exit 1
        fi
        echo "-> continuing deployment despite missing ci status."
    elif [[ "$CI_STATUS" == "success" ]]; then
        echo "-> ci success for commit $CURRENT_COMMIT_SHORT."
    else # Should not happen if logic above is correct
        echo "-> unknown ci status: '$CI_STATUS'. aborting."
        exit 1
    fi
fi

echo "-> continuing with deploy of $SERVICE_NAME."

# Update Kubernetes Deployment
echo "-> switching kubectl context to $TARGET_CLUSTER"
kubectl config use-context "$TARGET_CLUSTER"

KUBE_DEPLOYMENT_NAME="$SERVICE_NAME" # Assuming k8s deployment name matches service name
# Assuming container name inside deployment matches service name. Adjust if different.
KUBE_CONTAINER_NAME="$SERVICE_NAME" 
FULL_IMAGE_TAG="$IMAGE_BASE_NAME:$CURRENT_COMMIT_SHORT"

echo "-> updating deployment/$KUBE_DEPLOYMENT_NAME, container $KUBE_CONTAINER_NAME to image $FULL_IMAGE_TAG"
kubectl set image "deployment/$KUBE_DEPLOYMENT_NAME" "$KUBE_CONTAINER_NAME=$FULL_IMAGE_TAG" --record

# Update web-infra YAML only when deploying to staging or dev (smith cluster)
if [[ "$TARGET_CLUSTER" == "smith" ]]; then
    echo "-> updating web-infra yaml for $TARGET_ENV_NAME deployment."
    cd "$WEB_INFRA_DIR"

    # Check for dirty Git repository in web-infra
    if [ -n "$(git status --untracked-files=no --porcelain)" ]; then
        echo "-> web-infra repo is dirty, please commit changes before deploying"
        exit 1
    fi

    # Ensure we are on the main branch
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$CURRENT_BRANCH" != "main" ]]; then
        echo "-> web-infra repo is not on main branch. attempting to switch to main..."
        git checkout main
        if [ $? -ne 0 ]; then
            echo "-> failed to switch to main branch in web-infra. please resolve any issues and try again."
            exit 1
        fi
        echo "-> switched web-infra to main branch."
    fi

    echo "-> pulling latest web-infra changes from github"
    git pull --quiet

    DEPLOY_FILE_PATH=""
    if [[ "$CLUSTER" == "dev" ]]; then
        DEPLOY_FILE_PATH="$WEB_INFRA_DIR/overlays/dev/$SERVICE_NAME.yaml"
    elif [[ "$CLUSTER" == "stag" ]]; then
        DEPLOY_FILE_PATH="$WEB_INFRA_DIR/base/$SERVICE_NAME.yaml"
    else
        # This case should ideally not be reached if $TARGET_CLUSTER == "smith"
        # but as a safeguard:
        echo "-> error: web-infra update logic missing for cluster $CLUSTER on target $TARGET_CLUSTER"
        exit 1
    fi
    
    if [ ! -f "$DEPLOY_FILE_PATH" ]; then
        echo "-> error: deployment yaml file not found at $DEPLOY_FILE_PATH"
        exit 1
    fi
    
    echo "-> updating $DEPLOY_FILE_PATH image to $CURRENT_COMMIT_SHORT"
    # Note: The sed command uses | as a delimiter to avoid issues with paths/URLs in IMAGE_BASE_NAME
    sed -i.bak "s|$IMAGE_BASE_NAME:[a-f0-9]\{7,40\}|$FULL_IMAGE_TAG|" "$DEPLOY_FILE_PATH"
    rm -f "${DEPLOY_FILE_PATH}.bak" # Remove backup file created by sed -i on macOS

    echo "-> creating commit in web-infra"
    git add "$DEPLOY_FILE_PATH"
    COMMIT_MSG_ENV_TAG="dev"
    if [[ "$CLUSTER" == "stag" ]]; then
        COMMIT_MSG_ENV_TAG="stag"
    fi
    git commit -m "chore(eth-analysis-rs): update $SERVICE_NAME ($COMMIT_MSG_ENV_TAG) image to $CURRENT_COMMIT_SHORT"
    
    echo "-> pushing web-infra changes to github"
    git push --quiet
else
    echo "-> skipping web-infra yaml update (not deploying to $TARGET_ENV_NAME on $TARGET_CLUSTER)."
fi

echo "-> deployment of $SERVICE_NAME to $CLUSTER ($TARGET_CLUSTER) initiated."
osascript -e "display notification \"Deployed $SERVICE_NAME backend to $CLUSTER ($CURRENT_COMMIT_SHORT)\" with title \"Deployment Succeeded\""

echo "-> done." 