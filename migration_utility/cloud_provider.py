"""Multi-cloud abstraction — storage paths, identity, and provider detection.

Automatically detects the cloud provider from the Databricks workspace and
provides cloud-specific storage path generation, identity helpers, and
infrastructure operations.
"""

import os
from abc import ABC, abstractmethod
from log_config import get_logger

logger = get_logger(__name__)

_provider_instance = None


class CloudProvider(ABC):
    """Base class for cloud-specific operations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Cloud provider name: azure, aws, or gcp."""
        ...

    @abstractmethod
    def storage_root(self, account_or_bucket: str, container_or_prefix: str = "") -> str:
        """Return the root storage path for the given account/bucket."""
        ...

    @abstractmethod
    def volume_path(self, catalog: str, schema: str, volume: str) -> str:
        """Return the UC Volumes path."""
        ...

    @abstractmethod
    def external_location_url(self, account_or_bucket: str, container: str, path: str) -> str:
        """Return a full external location URL."""
        ...

    @abstractmethod
    def jdbc_driver_class(self) -> str:
        """Return the JDBC driver class for source SQL Server connectivity."""
        ...

    @abstractmethod
    def odbc_driver_name(self) -> str:
        """Return the ODBC driver name for pyodbc connections."""
        ...


class AzureProvider(CloudProvider):
    @property
    def name(self) -> str:
        return "azure"

    def storage_root(self, account_or_bucket: str, container_or_prefix: str = "datalake") -> str:
        return f"abfss://{container_or_prefix}@{account_or_bucket}.dfs.core.windows.net"

    def volume_path(self, catalog: str, schema: str, volume: str) -> str:
        return f"/Volumes/{catalog}/{schema}/{volume}"

    def external_location_url(self, account_or_bucket: str, container: str, path: str) -> str:
        return f"abfss://{container}@{account_or_bucket}.dfs.core.windows.net/{path.lstrip('/')}"

    def jdbc_driver_class(self) -> str:
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    def odbc_driver_name(self) -> str:
        return "ODBC Driver 18 for SQL Server"


class AWSProvider(CloudProvider):
    @property
    def name(self) -> str:
        return "aws"

    def storage_root(self, account_or_bucket: str, container_or_prefix: str = "") -> str:
        return f"s3://{account_or_bucket}"

    def volume_path(self, catalog: str, schema: str, volume: str) -> str:
        return f"/Volumes/{catalog}/{schema}/{volume}"

    def external_location_url(self, account_or_bucket: str, container: str, path: str) -> str:
        prefix = f"{container}/{path.lstrip('/')}" if container else path.lstrip("/")
        return f"s3://{account_or_bucket}/{prefix}"

    def jdbc_driver_class(self) -> str:
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    def odbc_driver_name(self) -> str:
        return "ODBC Driver 18 for SQL Server"


class GCPProvider(CloudProvider):
    @property
    def name(self) -> str:
        return "gcp"

    def storage_root(self, account_or_bucket: str, container_or_prefix: str = "") -> str:
        return f"gs://{account_or_bucket}"

    def volume_path(self, catalog: str, schema: str, volume: str) -> str:
        return f"/Volumes/{catalog}/{schema}/{volume}"

    def external_location_url(self, account_or_bucket: str, container: str, path: str) -> str:
        prefix = f"{container}/{path.lstrip('/')}" if container else path.lstrip("/")
        return f"gs://{account_or_bucket}/{prefix}"

    def jdbc_driver_class(self) -> str:
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    def odbc_driver_name(self) -> str:
        return "ODBC Driver 18 for SQL Server"


_PROVIDERS = {
    "azure": AzureProvider,
    "aws": AWSProvider,
    "gcp": GCPProvider,
}


def get_provider() -> CloudProvider:
    """Return the cloud provider instance.  Auto-detects from workspace if not set."""
    global _provider_instance
    if _provider_instance is not None:
        return _provider_instance

    cloud = os.environ.get("CLOUD_PROVIDER", "").lower()

    if not cloud:
        cloud = _detect_from_workspace()

    if cloud not in _PROVIDERS:
        logger.warning("Unknown cloud provider '%s', defaulting to azure", cloud)
        cloud = "azure"

    _provider_instance = _PROVIDERS[cloud]()
    logger.info("Cloud provider: %s", _provider_instance.name)
    return _provider_instance


def _detect_from_workspace() -> str:
    """Detect cloud from Databricks workspace URL or SDK config."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        host = (ws.config.host or "").lower()
        if ".azuredatabricks.net" in host:
            return "azure"
        if ".cloud.databricks.com" in host:
            return "aws"
        if ".gcp.databricks.com" in host:
            return "gcp"
    except Exception:
        pass

    dbx_host = os.environ.get("DATABRICKS_HOST", "").lower()
    if "azure" in dbx_host:
        return "azure"
    if "gcp" in dbx_host:
        return "gcp"
    return "aws"


def get_storage_url(account_or_bucket: str, container: str, path: str) -> str:
    """Convenience: build a full storage URL for the current cloud."""
    provider = get_provider()
    return provider.external_location_url(account_or_bucket, container, path)
