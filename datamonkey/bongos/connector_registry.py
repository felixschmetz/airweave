"""Registry for connector bongos with autodiscovery."""

import importlib
import inspect
import pkgutil
from typing import Dict, Type, List, Any

from datamonkey.bongos.base_bongo import BaseBongo
from datamonkey.utils.logging import get_logger


class ConnectorRegistry:
    """Registry for all available connector bongos."""

    _connectors: Dict[str, Type[BaseBongo]] = {}

    @classmethod
    def autodiscover(cls):
        """Discover and register bongos in this package automatically."""
        logger = get_logger("connector_registry")
        package = __package__  # datamonkey.bongos
        pkg_module = importlib.import_module(package)

        # Iterate modules within this package
        for _, mod_name, _ in pkgutil.iter_modules(pkg_module.__path__):
            try:
                module = importlib.import_module(f"{package}.{mod_name}")
            except Exception as e:
                logger.warning(f"Failed to import bongo module {mod_name}: {e}")
                continue

            for _, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, BaseBongo) and obj is not BaseBongo:
                    connector_type = getattr(obj, "connector_type", None)
                    if connector_type:
                        cls._connectors[connector_type] = obj
                        logger.info(f"✅ Registered connector: {connector_type}")

    @classmethod
    def get(cls, connector_type: str) -> Type[BaseBongo]:
        """Get a connector bongo class by type."""
        if not cls._connectors:
            cls.autodiscover()
        if connector_type not in cls._connectors:
            raise ValueError(f"Unknown connector type: {connector_type}")

        return cls._connectors[connector_type]

    @classmethod
    def list_available(cls) -> List[str]:
        """List all available connector types."""
        if not cls._connectors:
            cls.autodiscover()
        return list(cls._connectors.keys())

    @classmethod
    def create(cls, connector_type: str, credentials: Dict[str, Any], **kwargs) -> BaseBongo:
        """Create a new bongo instance."""
        bongo_class = cls.get(connector_type)
        return bongo_class(credentials, **kwargs)
