#!/bin/bash

###############################################################################
# Snowflake-META Streamlit Deployment Script
# 
# This script helps deploy the Streamlit app to Snowflake
# 
# Prerequisites:
# - SnowSQL installed and configured
# - Appropriate Snowflake permissions
# - Environment variables set (or pass as arguments)
###############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values (can be overridden by environment variables or arguments)
SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT:-}"
SNOWFLAKE_USER="${SNOWFLAKE_USER:-}"
SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"
SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-ANALYTICS}"
SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-STREAMLIT_APPS}"
APP_NAME="${APP_NAME:-SNOWMETA_UI}"
STAGE_NAME="${STAGE_NAME:-SNOWMETA_UI_STAGE}"

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy Snowflake-META Streamlit app to Snowflake

Options:
    -a, --account ACCOUNT       Snowflake account identifier
    -u, --user USER            Snowflake username
    -w, --warehouse WAREHOUSE  Snowflake warehouse (default: COMPUTE_WH)
    -d, --database DATABASE    Snowflake database (default: ANALYTICS)
    -s, --schema SCHEMA        Snowflake schema (default: STREAMLIT_APPS)
    -n, --name APP_NAME        Streamlit app name (default: SNOWMETA_UI)
    -h, --help                 Display this help message

Environment Variables:
    SNOWFLAKE_ACCOUNT          Snowflake account identifier
    SNOWFLAKE_USER             Snowflake username
    SNOWFLAKE_PASSWORD         Snowflake password
    SNOWFLAKE_WAREHOUSE        Snowflake warehouse
    SNOWFLAKE_DATABASE         Snowflake database
    SNOWFLAKE_SCHEMA           Snowflake schema

Example:
    $0 -a myaccount -u myuser -d ANALYTICS -s STREAMLIT_APPS
    
    # Or with environment variables:
    export SNOWFLAKE_ACCOUNT=myaccount
    export SNOWFLAKE_USER=myuser
    export SNOWFLAKE_PASSWORD=mypassword
    $0
EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -a|--account)
            SNOWFLAKE_ACCOUNT="$2"
            shift 2
            ;;
        -u|--user)
            SNOWFLAKE_USER="$2"
            shift 2
            ;;
        -w|--warehouse)
            SNOWFLAKE_WAREHOUSE="$2"
            shift 2
            ;;
        -d|--database)
            SNOWFLAKE_DATABASE="$2"
            shift 2
            ;;
        -s|--schema)
            SNOWFLAKE_SCHEMA="$2"
            shift 2
            ;;
        -n|--name)
            APP_NAME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$SNOWFLAKE_ACCOUNT" ]] || [[ -z "$SNOWFLAKE_USER" ]]; then
    print_error "Snowflake account and user are required"
    usage
    exit 1
fi

print_info "Starting Snowflake-META Streamlit deployment..."
echo

# Check if SnowSQL is installed
if command_exists snowsql; then
    print_success "SnowSQL found"
    DEPLOYMENT_METHOD="snowsql"
elif command_exists snow; then
    print_success "SnowCLI found"
    DEPLOYMENT_METHOD="snowcli"
else
    print_error "Neither SnowSQL nor SnowCLI found. Please install one of them."
    echo "  SnowSQL: https://docs.snowflake.com/en/user-guide/snowsql-install-config.html"
    echo "  SnowCLI: pip install snowflake-cli-labs"
    exit 1
fi

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
APP_FILE="$SCRIPT_DIR/app.py"

# Check if app.py exists
if [[ ! -f "$APP_FILE" ]]; then
    print_error "app.py not found at $APP_FILE"
    exit 1
fi

print_info "Deployment Configuration:"
echo "  Account:    $SNOWFLAKE_ACCOUNT"
echo "  User:       $SNOWFLAKE_USER"
echo "  Warehouse:  $SNOWFLAKE_WAREHOUSE"
echo "  Database:   $SNOWFLAKE_DATABASE"
echo "  Schema:     $SNOWFLAKE_SCHEMA"
echo "  App Name:   $APP_NAME"
echo "  App File:   $APP_FILE"
echo "  Method:     $DEPLOYMENT_METHOD"
echo

# Deploy using SnowSQL
if [[ "$DEPLOYMENT_METHOD" == "snowsql" ]]; then
    print_info "Deploying using SnowSQL..."
    
    # Create stage and upload file
    snowsql -a "$SNOWFLAKE_ACCOUNT" -u "$SNOWFLAKE_USER" -d "$SNOWFLAKE_DATABASE" -s "$SNOWFLAKE_SCHEMA" -w "$SNOWFLAKE_WAREHOUSE" <<EOF
-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA};

-- Create stage
CREATE STAGE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME}
    DIRECTORY = (ENABLE = TRUE);

-- Upload file
PUT file://${APP_FILE} @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Create Streamlit app
CREATE OR REPLACE STREAMLIT ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${APP_NAME}
    ROOT_LOCATION = '@${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME}'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = ${SNOWFLAKE_WAREHOUSE}
    COMMENT = 'Snowflake-META Pipeline Management UI';

-- Show created app
SHOW STREAMLIT APPS LIKE '${APP_NAME}';
EOF

    if [[ $? -eq 0 ]]; then
        print_success "Deployment completed successfully!"
    else
        print_error "Deployment failed"
        exit 1
    fi

# Deploy using SnowCLI
elif [[ "$DEPLOYMENT_METHOD" == "snowcli" ]]; then
    print_info "Deploying using SnowCLI..."
    
    snow streamlit deploy \
        --name "$APP_NAME" \
        --file "$APP_FILE" \
        --warehouse "$SNOWFLAKE_WAREHOUSE" \
        --database "$SNOWFLAKE_DATABASE" \
        --schema "$SNOWFLAKE_SCHEMA" \
        --replace
    
    if [[ $? -eq 0 ]]; then
        print_success "Deployment completed successfully!"
    else
        print_error "Deployment failed"
        exit 1
    fi
fi

echo
print_info "Access your Streamlit app:"
echo "  1. Log into Snowsight"
echo "  2. Navigate to: Projects > Streamlit"
echo "  3. Click on: $APP_NAME"
echo
print_success "Deployment complete! 🎉"

