"""Configuration utilities for Snowflake-META using Snowpark."""

import os
import json
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
from snowflake.snowpark import Session

logger = logging.getLogger("snowflake.labs.snowmeta")
logger.setLevel(logging.INFO)


@dataclass
class SnowparkConfig:
    """Configuration class for Snowpark session and settings."""
    
    session: Session
    database: str
    schema: str
    warehouse: str
    role: Optional[str] = None
    bronze_schema: str = "BRONZE"
    silver_schema: str = "SILVER"
    gold_schema: str = "GOLD"
    bronze_control_table: str = "bronze_control_table"
    silver_control_table: str = "silver_control_table"
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

@dataclass
class SnowflakeConfig:
    """Legacy configuration class for Snowflake connection and settings.
    
    DEPRECATED: Use SnowparkConfig instead for new implementations.
    """
    
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None
    region: Optional[str] = None
    authenticator: str = "snowflake"
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    session_parameters: Optional[Dict[str, Any]] = None


class SnowparkConfigManager:
    """Modern configuration manager for Snowflake-META using Snowpark."""
    
    def __init__(self, session: Optional[Session] = None):
        """Initialize Snowpark configuration manager.
        
        Args:
            session: Snowpark session. If None, will get or create one.
        """
        self.session = session or Session.builder.getOrCreate()
        self.config = self._get_session_config()
    
    def _get_session_config(self) -> Dict[str, Any]:
        """Get configuration from Snowpark session."""
        try:
            return {
                'database': self.session.get_current_database() or 'SNOWMETA_DB',
                'schema': self.session.get_current_schema() or 'PUBLIC',
                'warehouse': self.session.get_current_warehouse() or 'COMPUTE_WH',
                'role': self.session.get_current_role(),
                'bronze_schema': 'BRONZE',
                'silver_schema': 'SILVER',
                'gold_schema': 'GOLD',
                'bronze_control_table': 'bronze_control_table',
                'silver_control_table': 'silver_control_table',
                'log_level': 'INFO',
                'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        except Exception as e:
            logger.warning(f"Could not get session configuration: {str(e)}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration when session info is not available."""
        return {
            'database': 'SNOWMETA_DB',
            'schema': 'PUBLIC',
            'warehouse': 'COMPUTE_WH',
            'role': None,
            'bronze_schema': 'BRONZE',
            'silver_schema': 'SILVER',
            'gold_schema': 'GOLD',
            'bronze_control_table': 'bronze_control_table',
            'silver_control_table': 'silver_control_table',
            'log_level': 'INFO',
            'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }
    
    def get_snowpark_config(self) -> SnowparkConfig:
        """Get Snowpark configuration object."""
        return SnowparkConfig(
            session=self.session,
            database=self.config['database'],
            schema=self.config['schema'],
            warehouse=self.config['warehouse'],
            role=self.config.get('role'),
            bronze_schema=self.config['bronze_schema'],
            silver_schema=self.config['silver_schema'],
            gold_schema=self.config['gold_schema'],
            bronze_control_table=self.config['bronze_control_table'],
            silver_control_table=self.config['silver_control_table'],
            log_level=self.config['log_level'],
            log_format=self.config['log_format']
        )
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database-specific configuration."""
        return {
            'database': self.config['database'],
            'bronze_schema': self.config['bronze_schema'],
            'silver_schema': self.config['silver_schema'],
            'gold_schema': self.config['gold_schema'],
            'bronze_control_table': self.config['bronze_control_table'],
            'silver_control_table': self.config['silver_control_table'],
        }
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return {
            'log_level': self.config['log_level'],
            'log_file': None,
            'log_format': self.config['log_format']
        }
    
    def setup_logging(self):
        """Setup logging based on configuration."""
        logging_config = self.get_logging_config()
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, logging_config['log_level']),
            format=logging_config['log_format'],
            filename=logging_config.get('log_file'),
            filemode='a' if logging_config.get('log_file') else None
        )
        
        logger.info("Logging configured successfully for Snowpark session")


class ConfigManager:
    """Legacy configuration manager for Snowflake-META.
    
    DEPRECATED: Use SnowparkConfigManager instead for new implementations.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize configuration manager.
        
        Args:
            config_file: Path to configuration file (JSON or YAML)
        """
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or environment variables."""
        config = {}
        
        # Try to load from config file first
        if self.config_file and os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    if self.config_file.endswith('.json'):
                        config = json.load(f)
                    elif self.config_file.endswith(('.yml', '.yaml')):
                        import yaml
                        config = yaml.safe_load(f)
                    else:
                        logger.warning(f"Unsupported config file format: {self.config_file}")
            except Exception as e:
                logger.error(f"Error loading config file {self.config_file}: {str(e)}")
        
        # Override with environment variables
        config.update(self._load_from_env())
        
        return config
    
    def _load_from_env(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        env_config = {}
        
        # Snowflake connection parameters
        env_mappings = {
            'SNOWFLAKE_ACCOUNT': 'account',
            'SNOWFLAKE_USER': 'user',
            'SNOWFLAKE_PASSWORD': 'password',
            'SNOWFLAKE_WAREHOUSE': 'warehouse',
            'SNOWFLAKE_DATABASE': 'database',
            'SNOWFLAKE_SCHEMA': 'schema',
            'SNOWFLAKE_ROLE': 'role',
            'SNOWFLAKE_REGION': 'region',
            'SNOWFLAKE_AUTHENTICATOR': 'authenticator',
            'SNOWFLAKE_PRIVATE_KEY_PATH': 'private_key_path',
            'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE': 'private_key_passphrase',
        }
        
        for env_var, config_key in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                env_config[config_key] = value
        
        # Session parameters
        session_params = {}
        for key, value in os.environ.items():
            if key.startswith('SNOWFLAKE_SESSION_'):
                param_name = key.replace('SNOWFLAKE_SESSION_', '').lower()
                session_params[param_name] = value
        
        if session_params:
            env_config['session_parameters'] = session_params
        
        return env_config
    
    def get_snowflake_config(self) -> SnowflakeConfig:
        """Get Snowflake configuration object."""
        required_fields = ['account', 'user', 'password', 'warehouse', 'database', 'schema']
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required configuration field: {field}")
        
        return SnowflakeConfig(**self.config)
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters for Snowflake connector."""
        config = self.get_snowflake_config()
        
        connection_params = {
            'account': config.account,
            'user': config.user,
            'password': config.password,
            'warehouse': config.warehouse,
            'database': config.database,
            'schema': config.schema,
            'authenticator': config.authenticator,
        }
        
        # Add optional parameters
        if config.role:
            connection_params['role'] = config.role
        if config.region:
            connection_params['region'] = config.region
        if config.private_key_path:
            connection_params['private_key_path'] = config.private_key_path
        if config.private_key_passphrase:
            connection_params['private_key_passphrase'] = config.private_key_passphrase
        if config.session_parameters:
            connection_params['session_parameters'] = config.session_parameters
        
        return connection_params
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database-specific configuration."""
        return {
            'database': self.config.get('database', 'SNOWMETA_DB'),
            'bronze_schema': self.config.get('bronze_schema', 'BRONZE'),
            'silver_schema': self.config.get('silver_schema', 'SILVER'),
            'gold_schema': self.config.get('gold_schema', 'GOLD'),
            'bronze_control_table': self.config.get('bronze_control_table', 'bronze_control_table'),
            'silver_control_table': self.config.get('silver_control_table', 'silver_control_table'),
        }
    
    def get_warehouse_config(self) -> Dict[str, Any]:
        """Get warehouse configuration."""
        return {
            'warehouse': self.config.get('warehouse', 'COMPUTE_WH'),
            'auto_suspend': self.config.get('auto_suspend', 60),
            'auto_resume': self.config.get('auto_resume', True),
            'min_cluster_count': self.config.get('min_cluster_count', 1),
            'max_cluster_count': self.config.get('max_cluster_count', 1),
        }
    
    def get_security_config(self) -> Dict[str, Any]:
        """Get security configuration."""
        return {
            'encryption': self.config.get('encryption', True),
            'ssl_verify': self.config.get('ssl_verify', True),
            'insecure_mode': self.config.get('insecure_mode', False),
            'client_session_keep_alive': self.config.get('client_session_keep_alive', False),
        }
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return {
            'log_level': self.config.get('log_level', 'INFO'),
            'log_file': self.config.get('log_file', None),
            'log_format': self.config.get('log_format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
        }
    
    def validate_config(self) -> bool:
        """Validate configuration."""
        try:
            # Check required fields
            required_fields = ['account', 'user', 'password', 'warehouse', 'database', 'schema']
            for field in required_fields:
                if field not in self.config:
                    logger.error(f"Missing required configuration field: {field}")
                    return False
            
            # Validate warehouse configuration
            warehouse_config = self.get_warehouse_config()
            if warehouse_config['min_cluster_count'] > warehouse_config['max_cluster_count']:
                logger.error("min_cluster_count cannot be greater than max_cluster_count")
                return False
            
            # Validate security configuration
            security_config = self.get_security_config()
            if security_config['encryption'] and security_config['insecure_mode']:
                logger.warning("encryption is enabled but insecure_mode is also enabled")
            
            logger.info("Configuration validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Configuration validation failed: {str(e)}")
            return False
    
    def save_config(self, output_file: str):
        """Save current configuration to file."""
        try:
            with open(output_file, 'w') as f:
                if output_file.endswith('.json'):
                    json.dump(self.config, f, indent=4)
                elif output_file.endswith(('.yml', '.yaml')):
                    import yaml
                    yaml.dump(self.config, f, default_flow_style=False)
                else:
                    raise ValueError(f"Unsupported output file format: {output_file}")
            
            logger.info(f"Configuration saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving configuration to {output_file}: {str(e)}")
            raise


def create_snowpark_config(session: Optional[Session] = None) -> SnowparkConfig:
    """Create Snowpark configuration from session.
    
    Args:
        session: Snowpark session. If None, will get or create one.
        
    Returns:
        SnowparkConfig object with session-based configuration.
    """
    config_manager = SnowparkConfigManager(session)
    return config_manager.get_snowpark_config()

def create_default_config() -> Dict[str, Any]:
    """Create default configuration.
    
    DEPRECATED: Use create_snowpark_config() instead for new implementations.
    """
    import warnings
    warnings.warn(
        "create_default_config() is deprecated. Use create_snowpark_config() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return {
        'account': 'your_account.snowflakecomputing.com',
        'user': 'your_username',
        'password': 'your_password',
        'warehouse': 'COMPUTE_WH',
        'database': 'SNOWMETA_DB',
        'schema': 'PUBLIC',
        'role': 'ACCOUNTADMIN',
        'region': 'us-west-2',
        'authenticator': 'snowflake',
        'bronze_schema': 'BRONZE',
        'silver_schema': 'SILVER',
        'gold_schema': 'GOLD',
        'bronze_control_table': 'bronze_control_table',
        'silver_control_table': 'silver_control_table',
        'auto_suspend': 60,
        'auto_resume': True,
        'min_cluster_count': 1,
        'max_cluster_count': 1,
        'encryption': True,
        'ssl_verify': True,
        'insecure_mode': False,
        'client_session_keep_alive': False,
        'log_level': 'INFO',
        'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    }


def setup_snowpark_logging(session: Optional[Session] = None):
    """Setup logging for Snowpark session.
    
    Args:
        session: Snowpark session. If None, will get or create one.
    """
    config_manager = SnowparkConfigManager(session)
    config_manager.setup_logging()

def setup_logging(config_manager: ConfigManager):
    """Setup logging based on configuration.
    
    DEPRECATED: Use setup_snowpark_logging() instead for new implementations.
    """
    import warnings
    warnings.warn(
        "setup_logging() with ConfigManager is deprecated. Use setup_snowpark_logging() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    logging_config = config_manager.get_logging_config()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, logging_config['log_level']),
        format=logging_config['log_format'],
        filename=logging_config.get('log_file'),
        filemode='a' if logging_config.get('log_file') else None
    )
    
    logger.info("Logging configured successfully")


# Example usage and configuration templates
SNOWFLAKE_CONFIG_TEMPLATE = {
    "account": "your_account.snowflakecomputing.com",
    "user": "your_username", 
    "password": "your_password",
    "warehouse": "COMPUTE_WH",
    "database": "SNOWMETA_DB",
    "schema": "PUBLIC",
    "role": "ACCOUNTADMIN",
    "region": "us-west-2",
    "authenticator": "snowflake",
    "bronze_schema": "BRONZE",
    "silver_schema": "SILVER", 
    "gold_schema": "GOLD",
    "bronze_control_table": "bronze_control_table",
    "silver_control_table": "silver_control_table",
    "warehouse_settings": {
        "auto_suspend": 60,
        "auto_resume": True,
        "min_cluster_count": 1,
        "max_cluster_count": 1
    },
    "security_settings": {
        "encryption": True,
        "ssl_verify": True,
        "insecure_mode": False,
        "client_session_keep_alive": False
    },
    "logging_settings": {
        "log_level": "INFO",
        "log_file": None,
        "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    }
}

# Environment variable mappings for easy setup
ENVIRONMENT_VARIABLES = {
    'SNOWFLAKE_ACCOUNT': 'Snowflake account identifier',
    'SNOWFLAKE_USER': 'Snowflake username',
    'SNOWFLAKE_PASSWORD': 'Snowflake password',
    'SNOWFLAKE_WAREHOUSE': 'Snowflake warehouse name',
    'SNOWFLAKE_DATABASE': 'Snowflake database name',
    'SNOWFLAKE_SCHEMA': 'Snowflake schema name',
    'SNOWFLAKE_ROLE': 'Snowflake role name',
    'SNOWFLAKE_REGION': 'Snowflake region',
    'SNOWFLAKE_AUTHENTICATOR': 'Authentication method',
    'SNOWFLAKE_PRIVATE_KEY_PATH': 'Path to private key file',
    'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE': 'Private key passphrase'
}
