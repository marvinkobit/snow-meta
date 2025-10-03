-- Deploy Snowflake-META Streamlit App to Snowflake
-- 
-- Prerequisites:
-- 1. Upload app.py to a Snowflake stage
-- 2. Ensure you have appropriate privileges to create Streamlit apps
-- 3. Have a warehouse available for running the app

-- Step 1: Create a dedicated schema for the Streamlit app (optional)
USE DATABASE ANALYTICS;
CREATE SCHEMA IF NOT EXISTS STREAMLIT_APPS
    COMMENT = 'Schema for housing Streamlit applications';

-- Step 2: Create a stage for the Streamlit app files
CREATE OR REPLACE STAGE STREAMLIT_APPS.SNOWMETA_UI_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Snowflake-META Streamlit app files';

-- Step 3: Upload the app.py file to the stage
-- Use SnowSQL or Snowsight UI to upload:
-- PUT file:///path/to/ui/app.py @STREAMLIT_APPS.SNOWMETA_UI_STAGE AUTO_COMPRESS=FALSE;

-- Step 4: Create the Streamlit app
CREATE OR REPLACE STREAMLIT ANALYTICS.STREAMLIT_APPS.SNOWMETA_UI
    ROOT_LOCATION = '@ANALYTICS.STREAMLIT_APPS.SNOWMETA_UI_STAGE'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = COMPUTE_WH
    COMMENT = 'Snowflake-META Pipeline Management UI';

-- Step 5: Grant permissions to appropriate roles
GRANT USAGE ON STREAMLIT ANALYTICS.STREAMLIT_APPS.SNOWMETA_UI TO ROLE DATA_ENGINEER;
GRANT USAGE ON STREAMLIT ANALYTICS.STREAMLIT_APPS.SNOWMETA_UI TO ROLE ANALYST;

-- Step 6: Verify the app was created
SHOW STREAMLIT APPS IN SCHEMA STREAMLIT_APPS;

-- Step 7: Get the URL to access the app
DESC STREAMLIT ANALYTICS.STREAMLIT_APPS.SNOWMETA_UI;

-- Alternative: Create using inline code (Snowflake will store the code)
-- This approach doesn't require a stage

CREATE OR REPLACE STREAMLIT ANALYTICS.STREAMLIT_APPS.SNOWMETA_UI_INLINE
    FROM '/' -- Root location
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = COMPUTE_WH
AS
$$
-- Paste the entire contents of app.py here
-- (The app.py code would go between these markers)
$$
    COMMENT = 'Snowflake-META Pipeline Management UI (Inline)';

-- To update an existing Streamlit app:
-- 1. Upload new version to stage: PUT file:///path/to/ui/app.py @STREAMLIT_APPS.SNOWMETA_UI_STAGE OVERWRITE=TRUE;
-- 2. The app will automatically pick up changes

-- To drop the Streamlit app:
-- DROP STREAMLIT ANALYTICS.STREAMLIT_APPS.SNOWMETA_UI;

-- Monitoring and Management
-- Check app status
SHOW STREAMLIT APPS LIKE 'SNOWMETA%';

-- View app details
DESC STREAMLIT ANALYTICS.STREAMLIT_APPS.SNOWMETA_UI;

-- Check privileges
SHOW GRANTS ON STREAMLIT ANALYTICS.STREAMLIT_APPS.SNOWMETA_UI;

-- Access the app via Snowsight:
-- Navigate to: Projects > Streamlit > SNOWMETA_UI

