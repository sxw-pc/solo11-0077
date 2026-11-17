"""ShardRoute: offline shard-routing verification workbench."""

__version__ = "1.0.0"
SCHEMA_VERSION = 1
RUN_FORMAT = "shardroute-report/1"

from .errors import ShardRouteError, ConfigValidationError  # noqa: E402,F401

__all__ = [
    "__version__",
    "SCHEMA_VERSION",
    "RUN_FORMAT",
    "ShardRouteError",
    "ConfigValidationError",
]
