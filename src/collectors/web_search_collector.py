"""Collector for finding companies via web search and open catalogs."""
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import json
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import logger
from utils.helpers import rate_limit, clean_text
from config import RAW_DATA_DIR


class WebSearchCollector:
    """Collector for finding companies via open web sources."""
    
    # Известные BTL и маркетинговые агентства для seed list
    # Эти данные можно найти в открытых источниках
    KNOWN_COMPANIES = [
        # BTL агентства
        "Группа АДВ", "Eventum Premo", "iMARS Communications", "Deltaplan",
        "Komunica", "MarksMan", "Posterscope", "TWIGA", "Zebra",
        "MaximaX", "Event City", "Adrenalin Rush", "Action",
        
        # Агентства полного цикла
        "PR Inc", "Grape", "Affect", "Comunica", "CROS",
        "Михайлов и Партнеры", "Международная Панорама",
        
        # Коммуникационные группы
        "ID Media", "Publicity Russia", "Ketchum", "Edelman",
        
        # Сувенирная продукция / промо-материалы
        "Подарки.ру", "Прогресс", "Бизнес-Букет", "Панда",
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.companies = []
    
    def collect_from_known_list(self) -> List[Dict]:
        """Create initial seed list from known companies."""
        logger.info("Creating seed list from known companies...")
        
        companies = []
        
        # Категоризация компаний
        btl_keywords = ["group", "eventum", "imars", "deltaplan", "komunica", 
                        "marksman", "posterscope", "twiga", "zebra", "maximax",
                        "event", "adrenalin", "action"]
        
        full_cycle_keywords = ["pr inc", "grape", "affect", "comunica", "cros",
                               "михайлов", "панорама"]
        
        comm_group_keywords = ["id media", "publicity", "ketchum", "edelman"]
        
        souvenir_keywords = ["подарки", "прогресс", "букет", "панда"]
        
        for company_name in self.KNOWN_COMPANIES:
            name_lower = company_name.lower()
            
            # Определяем категорию
            if any(kw in name_lower for kw in souvenir_keywords):
                segment = "SOUVENIR"
            elif any(kw in name_lower for kw in comm_group_keywords):
                segment = "COMM_GROUP"
            elif any(kw in name_lower for kw in full_cycle_keywords):
                segment = "FULL_CYCLE"
            else:
                segment = "BTL"
            
            companies.append({
                'name': company_name,
                'segment_tag': segment,
                'source': 'known_list',
                'rating_ref': 'manual_seed'
            })
        
        logger.success(f"Created seed list with {len(companies)} companies")
        return companies
    
    def collect(self) -> List[Dict]:
        """Main collection method."""
        logger.info("Starting web search collection...")
        
        # Собираем seed list
        companies = self.collect_from_known_list()
        
        self.companies = companies
        self.save_raw_data()
        
        logger.success(f"Collected {len(companies)} companies via web search")
        return companies
    
    def save_raw_data(self):
        """Save raw data to JSON."""
        output_file = RAW_DATA_DIR / "web_search_raw.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.companies, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved raw web search data to {output_file}")


if __name__ == "__main__":
    # Тестовый запуск
    collector = WebSearchCollector()
    companies = collector.collect()
    print(f"\nCollected {len(companies)} companies")
    print("\nSample companies by segment:")
    
    from collections import Counter
    segments = Counter(c['segment_tag'] for c in companies)
    for segment, count in segments.items():
        print(f"  {segment}: {count} companies")
        sample = [c['name'] for c in companies if c['segment_tag'] == segment][:3]
        for name in sample:
            print(f"    - {name}")
