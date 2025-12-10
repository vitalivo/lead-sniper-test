"""Data cleaning and normalization."""
import pandas as pd
from typing import List, Dict
import re

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import logger
from utils.helpers import clean_text, clean_inn, normalize_revenue


class DataCleaner:
    """Clean and normalize company data."""
    
    def __init__(self):
        pass
    
    def clean_companies(self, companies: List[Dict]) -> pd.DataFrame:
        """Clean and normalize company data."""
        logger.info(f"Cleaning {len(companies)} companies...")
        
        # Конвертируем в DataFrame
        df = pd.DataFrame(companies)
        
        initial_count = len(df)
        
        # Очистка названий
        if 'name' in df.columns:
            df['name'] = df['name'].apply(lambda x: clean_text(str(x)) if pd.notna(x) else '')
            df = df[df['name'] != '']
        
        # Очистка ИНН
        if 'inn' in df.columns:
            df['inn'] = df['inn'].apply(lambda x: clean_inn(str(x)) if pd.notna(x) else '')
        
        # Нормализация выручки
        if 'revenue' in df.columns:
            df['revenue'] = df['revenue'].apply(
                lambda x: normalize_revenue(str(x)) if pd.notna(x) else 0
            )
        
        # Убедимся что revenue_year это число
        if 'revenue_year' in df.columns:
            df['revenue_year'] = pd.to_numeric(df['revenue_year'], errors='coerce')
            # Если год не указан, ставим 2023 (последний доступный)
            df['revenue_year'] = df['revenue_year'].fillna(2023).astype(int)
        
        # Очистка ОКВЭД
        if 'okved_main' in df.columns:
            df['okved_main'] = df['okved_main'].apply(
                lambda x: clean_text(str(x)) if pd.notna(x) else ''
            )
        
        # Очистка региона
        if 'region' in df.columns:
            df['region'] = df['region'].apply(
                lambda x: clean_text(str(x)) if pd.notna(x) else ''
            )
        
        # Очистка сайта
        if 'site' in df.columns:
            df['site'] = df['site'].apply(self._clean_url)
        
        # Очистка описания
        if 'description' in df.columns:
            df['description'] = df['description'].apply(
                lambda x: clean_text(str(x)) if pd.notna(x) else ''
            )
        
        # Убираем строки без названия
        df = df[df['name'].notna() & (df['name'] != '')]
        
        logger.success(f"Cleaned data: {len(df)}/{initial_count} companies remaining")
        
        return df
    
    def _clean_url(self, url):
        """Clean and validate URL."""
        if pd.isna(url) or not url:
            return ''
        
        url = str(url).strip()
        
        # Убираем пробелы
        url = url.replace(' ', '')
        
        # Добавляем http:// если нет протокола
        if url and not url.startswith(('http://', 'https://')):
            url = 'http://' + url
        
        return url
    
    def ensure_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all required columns exist."""
        required_columns = {
            'inn': '',
            'name': '',
            'revenue_year': 2023,
            'revenue': 0,
            'segment_tag': '',
            'source': ''
        }
        
        for col, default_value in required_columns.items():
            if col not in df.columns:
                df[col] = default_value
                logger.warning(f"Added missing column: {col}")
        
        return df


if __name__ == "__main__":
    # Тест
    import json
    from config import RAW_DATA_DIR
    
    # Загружаем тестовые данные
    with open(RAW_DATA_DIR / "web_search_raw.json", 'r', encoding='utf-8') as f:
        companies = json.load(f)
    
    cleaner = DataCleaner()
    df = cleaner.clean_companies(companies)
    df = cleaner.ensure_required_columns(df)
    
    print("\nCleaned DataFrame:")
    print(df.head())
    print(f"\nColumns: {df.columns.tolist()}")
    print(f"\nShape: {df.shape}")
