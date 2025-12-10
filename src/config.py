"""Configuration management for the project."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
OUTPUT_FILE = DATA_DIR / "companies.csv"

# Create directories if they don't exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DATA_DIR.mkdir(parents=True, exist_ok=True)

# API Keys
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
RUSPROFILE_API_KEY = os.getenv("RUSPROFILE_API_KEY", "")

# Rate limiting
MAX_REQUESTS_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "10"))
REQUEST_DELAY_SECONDS = int(os.getenv("REQUEST_DELAY_SECONDS", "2"))

# Filters
MIN_REVENUE = int(os.getenv("MIN_REVENUE", "200000000"))
TARGET_COUNTRY = os.getenv("TARGET_COUNTRY", "Russia")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = BASE_DIR / os.getenv("LOG_FILE", "app.log")

# LLM settings
USE_LLM = os.getenv("USE_LLM", "false").lower() == "true"
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "anthropic")
LLM_MODEL = os.getenv("LLM_MODEL", "claude-sonnet-4-20250514")

# Segment tags
SEGMENT_KEYWORDS = {
    "BTL": ["btl", "промо", "промоутер", "мерчендайзинг", "активация", "brand activation"],
    "EVENT": ["ивент", "event", "мероприятие", "выставк", "конференц"],
    "SOUVENIR": ["сувенир", "промо-продукция", "промо-материал", "брендирован", "сувенирная продукция"],
    "FULL_CYCLE": ["полный цикл", "комплексн", "интегрированн", "360", "full cycle", "full-service"],
    "COMM_GROUP": ["группа", "холдинг", "коммуникационн", "group", "holding"]
}

# Relevant OKVED codes
RELEVANT_OKVED = [
    "73.11",  # Деятельность рекламных агентств
    "73.12",  # Представление в средствах массовой информации
    "82.30",  # Деятельность по организации конференций и выставок
    "74.20",  # Деятельность в области фотографии
    "32.99",  # Производство прочих готовых изделий
    "46.49",  # Торговля оптовая прочими бытовыми товарами
]

# Data sources
SOURCES = {
    "RRAR_2025": "https://www.raso.ru/",  # Placeholder
    "RUWARD": "https://ruward.ru/",
    "RUSPROFILE": "https://www.rusprofile.ru/",
    "LIST_ORG": "https://www.list-org.com/",
}
