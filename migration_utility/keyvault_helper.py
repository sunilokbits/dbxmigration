"""Backward-compat shim — re-exports from secrets_helper.py.

All 35+ import sites across the codebase (from keyvault_helper import ...)
continue to resolve without changes.  The original Azure Key Vault logic
has been replaced by Databricks Secrets in secrets_helper.py.
"""
from secrets_helper import (  # noqa: F401
    get_secret,
    set_secret,
    get_source_password,
    set_source_password,
    get_databricks_token,
    set_databricks_token,
    get_devops_token,
    set_devops_token,
    clear_cache,
    is_masked,
    MASKED_VALUE,
)
