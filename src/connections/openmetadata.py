"""OpenMetadata SDK integration (optional dependency)."""

import logging
from typing import Any

from src.config import OpenMetadataConfig

logger = logging.getLogger(__name__)


class OpenMetadataConnection:
    def __init__(self, config: OpenMetadataConfig):
        self._config = config
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client

        if not self._config.enabled:
            return None

        try:
            from metadata.ingestion.ometa.ometa_api import OpenMetadata
            from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
                OpenMetadataConnection as OMConnection,
                AuthProvider,
            )
            from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
                OpenMetadataJWTClientConfig,
            )

            config = OMConnection(
                hostPort=self._config.server_url,
                authProvider=AuthProvider.openmetadata,
                securityConfig=OpenMetadataJWTClientConfig(
                    jwtToken=self._config.api_token
                ),
            )
            self._client = OpenMetadata(config)
            return self._client
        except ImportError:
            logger.warning("openmetadata-ingestion not installed. "
                          "Install with: pip install 'synthetic-data-profiler[openmetadata]'")
            return None

    def get_tables(self) -> list[dict[str, Any]]:
        """Get all tables from OpenMetadata."""
        client = self._get_client()
        if client is None:
            return []

        try:
            from metadata.generated.schema.entity.data.table import Table

            tables = client.list_entities(entity=Table, limit=1000)
            result = []
            for entity in tables.entities:
                result.append({
                    "name": entity.name.__root__,
                    "fqn": entity.fullyQualifiedName.__root__ if entity.fullyQualifiedName else None,
                })
            return result
        except Exception as e:
            logger.error("Failed to fetch tables from OpenMetadata: %s", e)
            return []
