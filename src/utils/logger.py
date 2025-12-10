"""Logging utility."""
from loguru import logger
import sys
from config import LOG_LEVEL, LOG_FILE

# Remove default handler
logger.remove()

# Add console handler
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    level=LOG_LEVEL,
    colorize=True,
)

# Add file handler
logger.add(
    LOG_FILE,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
    level=LOG_LEVEL,
    rotation="10 MB",
    retention="1 week",
    compression="zip",
)

__all__ = ["logger"]
