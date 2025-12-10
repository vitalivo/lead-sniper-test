"""Deduplication of company records."""
import pandas as pd
from typing import List

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import logger


class Deduplicator:
    """Remove duplicate company records."""
    
    def __init__(self):
        pass
    
    def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicates based on INN and name."""
        logger.info(f"Starting deduplication. Initial records: {len(df)}")
        
        initial_count = len(df)
        
        # Создаём копию для работы
        df_dedup = df.copy()
        
        # 1. Дедупликация по ИНН (приоритетнее)
        if 'inn' in df_dedup.columns:
            # Оставляем записи с ИНН
            df_with_inn = df_dedup[df_dedup['inn'].notna() & (df_dedup['inn'] != '')]
            df_without_inn = df_dedup[df_dedup['inn'].isna() | (df_dedup['inn'] == '')]
            
            # Дедупликация по ИНН - оставляем запись с максимальной полнотой данных
            if len(df_with_inn) > 0:
                df_with_inn = df_with_inn.sort_values(
                    by=['revenue', 'revenue_year'], 
                    ascending=[False, False]
                )
                df_with_inn = df_with_inn.drop_duplicates(subset=['inn'], keep='first')
                
                logger.info(f"Deduplicated by INN: {len(df_with_inn)} unique INNs")
            
            # Объединяем обратно
            df_dedup = pd.concat([df_with_inn, df_without_inn], ignore_index=True)
        
        # 2. Дедупликация по названию (для записей без ИНН)
        if 'name' in df_dedup.columns:
            # Нормализуем названия для сравнения
            df_dedup['name_normalized'] = df_dedup['name'].str.lower().str.strip()
            
            # Для записей без ИНН дедуплицируем по имени
            mask_no_inn = df_dedup['inn'].isna() | (df_dedup['inn'] == '')
            
            if mask_no_inn.sum() > 0:
                df_no_inn = df_dedup[mask_no_inn]
                df_no_inn = df_no_inn.drop_duplicates(subset=['name_normalized'], keep='first')
                
                df_with_inn = df_dedup[~mask_no_inn]
                
                df_dedup = pd.concat([df_with_inn, df_no_inn], ignore_index=True)
                
                logger.info(f"Deduplicated by name: {len(df_no_inn)} unique names without INN")
            
            # Удаляем временную колонку
            df_dedup = df_dedup.drop(columns=['name_normalized'])
        
        # 3. Удаляем полные дубликаты (на всякий случай)
        df_dedup = df_dedup.drop_duplicates()
        
        removed_count = initial_count - len(df_dedup)
        logger.success(f"Deduplication complete. Removed {removed_count} duplicates. Final: {len(df_dedup)} records")
        
        return df_dedup


if __name__ == "__main__":
    # Тест
    test_data = [
        {'inn': '1234567890', 'name': 'Company A', 'revenue': 1000000},
        {'inn': '1234567890', 'name': 'Company A', 'revenue': 2000000},  # Дубликат по ИНН
        {'inn': '', 'name': 'Company B', 'revenue': 500000},
        {'inn': '', 'name': 'Company B', 'revenue': 600000},  # Дубликат по имени
        {'inn': '9876543210', 'name': 'Company C', 'revenue': 300000},
    ]
    
    df = pd.DataFrame(test_data)
    print("Original DataFrame:")
    print(df)
    
    dedup = Deduplicator()
    df_dedup = dedup.deduplicate(df)
    
    print("\nDeduplicated DataFrame:")
    print(df_dedup)
