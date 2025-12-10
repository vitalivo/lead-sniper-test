"""Validation and filtering of company data."""
import pandas as pd
from typing import List, Set

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import logger
from config import MIN_REVENUE, TARGET_COUNTRY, RELEVANT_OKVED, SEGMENT_KEYWORDS


class DataValidator:
    """Validate and filter company data."""
    
    def __init__(self, min_revenue: int = MIN_REVENUE):
        self.min_revenue = min_revenue
    
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all validation rules."""
        logger.info(f"Starting validation. Initial records: {len(df)}")
        
        initial_count = len(df)
        
        # 1. Фильтр по стране (Россия)
        df = self._filter_by_country(df)
        
        # 2. Фильтр по выручке
        df = self._filter_by_revenue(df)
        
        # 3. Валидация сегмента
        df = self._validate_segment(df)
        
        # 4. Удаление недействующих компаний
        df = self._filter_active_companies(df)
        
        final_count = len(df)
        removed = initial_count - final_count
        
        logger.success(f"Validation complete. Removed {removed} records. Final: {final_count} records")
        
        return df
    
    def _filter_by_country(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter companies by country (Russia)."""
        if 'region' not in df.columns:
            logger.warning("No 'region' column, skipping country filter")
            return df
        
        initial = len(df)
        
        # Российские регионы (примерный список)
        russian_keywords = [
            'москва', 'санкт-петербург', 'спб', 'россия', 'russia',
            'екатеринбург', 'новосибирск', 'казань', 'нижний новгород',
            'челябинск', 'самара', 'ростов', 'уфа', 'красноярск',
            'пермь', 'воронеж', 'волгоград', 'краснодар', 'саратов'
        ]
        
        # Фильтруем по региону
        mask = df['region'].str.lower().str.contains('|'.join(russian_keywords), na=False)
        
        # Если регион не указан, оставляем запись (будем считать что это РФ)
        mask = mask | df['region'].isna() | (df['region'] == '')
        
        df_filtered = df[mask]
        
        removed = initial - len(df_filtered)
        if removed > 0:
            logger.info(f"Filtered by country: removed {removed} non-Russian companies")
        
        return df_filtered
    
    def _filter_by_revenue(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter companies by minimum revenue."""
        if 'revenue' not in df.columns:
            logger.warning("No 'revenue' column, skipping revenue filter")
            return df
        
        initial = len(df)
        
        # Фильтруем по выручке
        df_filtered = df[df['revenue'] >= self.min_revenue]
        
        removed = initial - len(df_filtered)
        logger.info(f"Filtered by revenue (>= {self.min_revenue:,}): removed {removed} companies")
        
        return df_filtered
    
    def _validate_segment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate that segment_tag is relevant."""
        if 'segment_tag' not in df.columns:
            logger.warning("No 'segment_tag' column, skipping segment validation")
            return df
        
        initial = len(df)
        
        # Допустимые сегменты
        valid_segments = set(SEGMENT_KEYWORDS.keys())
        
        # Оставляем только записи с валидными сегментами
        df_filtered = df[df['segment_tag'].isin(valid_segments)]
        
        removed = initial - len(df_filtered)
        if removed > 0:
            logger.info(f"Segment validation: removed {removed} companies with invalid segments")
        
        return df_filtered
    
    def _filter_active_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove inactive/liquidated companies."""
        if 'status' not in df.columns:
            logger.warning("No 'status' column, skipping active company filter")
            return df
        
        initial = len(df)
        
        # Фильтруем ликвидированные компании
        inactive_keywords = ['ликвидирован', 'банкротств', 'реорганизац', 'liquidated', 'bankrupt']
        
        mask = ~df['status'].str.lower().str.contains('|'.join(inactive_keywords), na=False)
        
        # Если статус не указан, оставляем (считаем активной)
        mask = mask | df['status'].isna() | (df['status'] == '')
        
        df_filtered = df[mask]
        
        removed = initial - len(df_filtered)
        if removed > 0:
            logger.info(f"Filtered inactive companies: removed {removed} companies")
        
        return df_filtered
    
    def check_relevance_by_okved(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mark companies as relevant based on OKVED codes."""
        if 'okved_main' not in df.columns:
            logger.warning("No 'okved_main' column for OKVED check")
            return df
        
        def is_relevant_okved(okved: str) -> bool:
            if pd.isna(okved) or okved == '':
                return False
            
            for relevant_code in RELEVANT_OKVED:
                if okved.startswith(relevant_code):
                    return True
            return False
        
        df['okved_relevant'] = df['okved_main'].apply(is_relevant_okved)
        
        relevant_count = df['okved_relevant'].sum()
        logger.info(f"OKVED relevance check: {relevant_count}/{len(df)} companies have relevant OKVED")
        
        return df


if __name__ == "__main__":
    # Тест
    test_data = [
        {'inn': '1234567890', 'name': 'Company A', 'revenue': 250000000, 'region': 'Москва', 'segment_tag': 'BTL', 'status': 'Действующая'},
        {'inn': '2345678901', 'name': 'Company B', 'revenue': 150000000, 'region': 'Москва', 'segment_tag': 'BTL', 'status': 'Действующая'},
        {'inn': '3456789012', 'name': 'Company C', 'revenue': 300000000, 'region': 'USA', 'segment_tag': 'BTL', 'status': 'Действующая'},
        {'inn': '4567890123', 'name': 'Company D', 'revenue': 500000000, 'region': 'Москва', 'segment_tag': 'INVALID', 'status': 'Действующая'},
        {'inn': '5678901234', 'name': 'Company E', 'revenue': 400000000, 'region': 'Москва', 'segment_tag': 'BTL', 'status': 'Ликвидирована'},
    ]
    
    df = pd.DataFrame(test_data)
    print("Original DataFrame:")
    print(df)
    print(f"\nCount: {len(df)}")
    
    validator = DataValidator(min_revenue=200000000)
    df_valid = validator.validate(df)
    
    print("\nValidated DataFrame:")
    print(df_valid)
    print(f"\nCount: {len(df_valid)}")
