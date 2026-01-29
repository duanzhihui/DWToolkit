"""转换规则模块"""

from .api_changes import API_CHANGES, APIChange
from .config_updates import CONFIG_UPDATES, ConfigUpdate
from .deprecations import DEPRECATED_APIS, DeprecatedAPI
from .syntax_changes import SYNTAX_CHANGES, SyntaxChange

__all__ = [
    "API_CHANGES",
    "APIChange",
    "CONFIG_UPDATES",
    "ConfigUpdate",
    "DEPRECATED_APIS",
    "DeprecatedAPI",
    "SYNTAX_CHANGES",
    "SyntaxChange",
]
