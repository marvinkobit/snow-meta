
import copy
import dataclasses
import json
import yaml
import logging
import ast
from typing import Dict, Any, List, Optional

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, current_timestamp
from snowflake.snowpark.types import StructType, StructField, StringType, VariantType, TimestampType



class SnowmetaPipeline:
    pass