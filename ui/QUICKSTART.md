# Snowflake-META UI Quick Start Guide

Get started with the Snowflake-META Streamlit application in minutes!

## 🚀 Quick Start (Local Development)

### 1. Install Dependencies

```bash
cd /home/raport/codes/DE/snow-meta/ui
pip install -r requirements.txt
```

### 2. Run the Application

```bash
streamlit run app.py
```

The app will automatically open in your browser at `http://localhost:8501`

## ☁️ Deploy to Snowflake

### Option 1: Automated Deployment (Using deploy.sh)

```bash
# Set environment variables
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password

# Run deployment script
./deploy.sh
```

### Option 2: Manual Deployment (Using Snowsight UI)

1. Log into [Snowsight](https://app.snowflake.com)
2. Navigate to **Projects** > **Streamlit**
3. Click **+ Streamlit App**
4. Configure:
   - **Name**: `SNOWMETA_UI`
   - **Warehouse**: Select your warehouse
   - **Database**: Select target database
   - **Schema**: Select or create schema
5. Copy/paste the contents of `app.py` into the editor
6. Click **Run**

### Option 3: Using SQL Commands

```bash
# Upload file using SnowSQL
snowsql -a your_account -u your_username
```

Then run the commands from `deploy_to_snowflake.sql`

## 📖 Using the Application

### Step 1: Onboarding

1. Open the application
2. Navigate to **UI** tab > **Step 1: Onboarding**
3. Fill in required fields:
   - **Database name**: Your target Snowflake database
   - **Schema names**: Bronze and silver layer schemas
   - **Onboarding file path**: Stage path to config file (e.g., `@MY_STAGE/config.json`)
4. Click **Run Onboarding**

### Step 2: Deployment

1. After completing onboarding, go to **Step 2: Deployment**
2. Review auto-populated fields
3. Enter additional required fields:
   - **Target schema name**: Where data will be written
   - **Pipeline name**: Unique identifier for your pipeline
4. Click **Deploy**

### Demo Tab

- View sample JSON configurations
- Learn about Snowflake concepts
- Understand pipeline architecture

### CLI Tab

- Python code examples
- SQL command references
- Monitoring queries

## 🔧 Configuration

### Snowflake Connection (for local development)

Create a `secrets.toml` file in `.streamlit/` directory:

```toml
[connections.snowflake]
account = "your_account"
user = "your_username"
password = "your_password"
warehouse = "COMPUTE_WH"
database = "ANALYTICS"
schema = "PUBLIC"
role = "DATA_ENGINEER"
```

**Note**: This file is only needed for local development. When deployed to Snowflake, the app automatically uses the Snowflake session.

## 🎨 Customization

### Theme Configuration

Edit `.streamlit/config.toml` to customize colors and appearance:

```toml
[theme]
primaryColor = "#29B5E8"  # Snowflake blue
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
textColor = "#262730"
```

### App Configuration

Modify `app.py` to:
- Add custom fields
- Change default values
- Add validation rules
- Integrate with your workflows

## 📊 Key Mappings: Databricks → Snowflake

| Databricks Term | Snowflake Equivalent | Used In App |
|----------------|---------------------|-------------|
| Unity Catalog | Database | Database field |
| DBFS | Stages/Volumes | File path fields |
| DLT | Streams + Tasks | Pipeline deployment |
| Delta Lake | Native Tables | Storage layer |
| Metastore | Information Schema | Metadata queries |
| Workspace | Account/Database | Connection config |
| Serverless DLT | Serverless Tasks | Serverless option |
| Cluster | Warehouse | Compute selection |

## 🐛 Troubleshooting

### Port Already in Use

```bash
# Kill existing Streamlit process
pkill -f streamlit

# Or use a different port
streamlit run app.py --server.port 8502
```

### Import Errors

```bash
# Ensure you're in the correct directory
cd /home/raport/codes/DE/snow-meta/ui

# Reinstall dependencies
pip install -r requirements.txt --upgrade
```

### Snowflake Connection Issues

```bash
# Test connection using Python
python -c "
from snowflake.snowpark import Session
session = Session.builder.configs({
    'account': 'your_account',
    'user': 'your_username',
    'password': 'your_password',
    'warehouse': 'COMPUTE_WH'
}).create()
print('Connected successfully!')
session.close()
"
```

### Stage Path Not Found

Ensure your stage exists and contains files:

```sql
-- Create stage if it doesn't exist
CREATE STAGE IF NOT EXISTS MY_STAGE;

-- Upload files to stage
PUT file:///path/to/local/file.json @MY_STAGE;

-- List files in stage
LIST @MY_STAGE;
```

## 📚 Additional Resources

- [Streamlit Documentation](https://docs.streamlit.io/)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- [Snowpark Python API](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)
- [Snowflake Stages](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage.html)
- [Snowflake Streams](https://docs.snowflake.com/en/user-guide/streams.html)
- [Snowflake Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro.html)

## 💡 Tips

1. **Start Local**: Test configurations locally before deploying to Snowflake
2. **Use Stages**: Store configuration files in Snowflake stages for easy access
3. **Version Control**: Keep onboarding JSON files in version control
4. **Test Incrementally**: Test bronze layer before moving to silver
5. **Monitor Tasks**: Regularly check task execution history in Snowflake

## 🔐 Security Best Practices

1. **Never commit credentials**: Use environment variables or Snowflake secrets
2. **Use appropriate roles**: Grant minimum necessary privileges
3. **Secure stages**: Use encrypted stages for sensitive data
4. **Audit access**: Enable Snowflake audit logging
5. **Rotate credentials**: Regularly update passwords and keys

## 🎯 Next Steps

1. ✅ Complete onboarding for your first data source
2. ✅ Deploy a test pipeline
3. ✅ Monitor execution in Snowflake
4. ✅ Scale to multiple data sources
5. ✅ Set up alerts and monitoring

## 📞 Support

For issues or questions:
- Check the **Demo** tab for examples
- Review the **CLI** tab for command references  
- Consult main project documentation

---

**Happy Data Engineering with Snowflake-META! ❄️**

