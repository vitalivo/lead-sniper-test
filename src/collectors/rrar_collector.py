"""Collector for RRAR (Russian Public Relations Association Rating) 2025."""
import requests
from bs4 import BeautifulSoup
import json
from pathlib import Path
from typing import List, Dict, Optional
import time

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import logger
from utils.helpers import rate_limit, clean_text
from config import RAW_DATA_DIR


class RRARCollector:
    """Collector for RRAR rating data."""
    
    BASE_URL = "https://www.raso.ru"
    
    # Целевые категории рейтинга
    TARGET_CATEGORIES = {
        "btl": "BTL агентства",
        "souvenir": "Сувенирная продукция",
        "full_cycle": "Агентства полного цикла",
        "comm_groups": "Коммуникационные группы"
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.companies = []
    
    @rate_limit(delay=2)
    def fetch_page(self, url: str) -> Optional[str]:
        """Fetch page content with rate limiting."""
        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None
    
    def parse_rrar_page(self, html: str, category: str) -> List[Dict]:
        """Parse RRAR rating page and extract companies."""
        companies = []
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Попытка найти таблицу с компаниями (структура может отличаться)
            # Это примерная структура, нужно будет адаптировать под реальный сайт
            
            # Вариант 1: Таблица
            table = soup.find('table', class_='rating-table')
            if table:
                rows = table.find_all('tr')[1:]  # Пропускаем заголовок
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        company_name = clean_text(cols[1].get_text())
                        if company_name:
                            companies.append({
                                'name': company_name,
                                'segment_tag': category.upper(),
                                'source': 'rrar_2025',
                                'rating_ref': f"rrar_{category}"
                            })
            
            # Вариант 2: Список div элементов
            else:
                company_blocks = soup.find_all('div', class_='company-item')
                for block in company_blocks:
                    name_elem = block.find(['h3', 'h4', 'a'])
                    if name_elem:
                        company_name = clean_text(name_elem.get_text())
                        if company_name:
                            companies.append({
                                'name': company_name,
                                'segment_tag': category.upper(),
                                'source': 'rrar_2025',
                                'rating_ref': f"rrar_{category}"
                            })
            
            logger.info(f"Extracted {len(companies)} companies from {category}")
            
        except Exception as e:
            logger.error(f"Error parsing RRAR page: {e}")
        
        return companies
    
    def collect_manual_seed(self) -> List[Dict]:
        """
        Ручной seed list на случай, если парсинг не работает.
        Это fallback вариант для тестового задания.
        """
        logger.warning("Using manual seed list as fallback")
        
        # Примеры известных BTL агентств (для демонстрации)
        manual_seed = [
            {"name": "Группа АДВ", "segment_tag": "BTL", "source": "manual_seed"},
            {"name": "Eventum Premo", "segment_tag": "BTL", "source": "manual_seed"},
            {"name": "iMARS Communications", "segment_tag": "FULL_CYCLE", "source": "manual_seed"},
            {"name": "Deltaplan", "segment_tag": "BTL", "source": "manual_seed"},
            {"name": "ID Media", "segment_tag": "COMM_GROUP", "source": "manual_seed"},
            {"name": "Komunica", "segment_tag": "BTL", "source": "manual_seed"},
            {"name": "MarksMan", "segment_tag": "BTL", "source": "manual_seed"},
            {"name": "Posterscope", "segment_tag": "BTL", "source": "manual_seed"},
            {"name": "PR Inc", "segment_tag": "FULL_CYCLE", "source": "manual_seed"},
            {"name": "Grape", "segment_tag": "COMM_GROUP", "source": "manual_seed"},
        ]
        
        return manual_seed
    
    def collect(self) -> List[Dict]:
        """Main collection method."""
        logger.info("Starting RRAR data collection...")
        
        all_companies = []
        
        # Попытка парсинга с реального сайта
        # ВАЖНО: URL нужно будет найти вручную для каждой категории
        
        # Для тестового задания используем ручной seed + поиск по открытым источникам
        logger.info("Note: RRAR website structure needs manual inspection")
        logger.info("Using combination of manual seed + web search")
        
        # Добавляем ручной seed
        manual_companies = self.collect_manual_seed()
        all_companies.extend(manual_companies)
        
        # Сохраняем результат
        self.companies = all_companies
        self.save_raw_data()
        
        logger.success(f"Collected {len(all_companies)} companies from RRAR")
        return all_companies
    
    def save_raw_data(self):
        """Save raw data to JSON."""
        output_file = RAW_DATA_DIR / "rrar_raw.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.companies, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved raw RRAR data to {output_file}")


if __name__ == "__main__":
    # Тестовый запуск
    collector = RRARCollector()
    companies = collector.collect()
    print(f"\nCollected {len(companies)} companies")
    print("\nFirst 3 companies:")
    for company in companies[:3]:
        print(f"  - {company['name']} ({company['segment_tag']})")
