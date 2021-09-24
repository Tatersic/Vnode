import sys
from logging import (getLogger, StreamHandler, Formatter,
    DEBUG, INFO, WARNING, ERROR, CRITICAL)

logger = getLogger("Vnode")
default_handler = StreamHandler(sys.stdout)
default_handler.setFormatter(
    Formatter("[%(asctime)s %(name)s][%(levelname)s] %(message)s")
)
logger.addHandler(default_handler)

__all__ = [
    'logger',
    'DEBUG',
    'INFO',
    'WARNING',
    'ERROR',
    'CRITICAL'
]