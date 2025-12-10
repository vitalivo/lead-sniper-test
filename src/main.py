"""Main entry point for the data collection pipeline."""
import sys
from pathlib import Path
import pandas as pd
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.logger import logger
from config import OUTPUT_FILE, MIN_REVENUE

# Processors
from processors.cleaner import DataCleaner
from processors.deduplicator import Deduplicator
from processors.validator import DataValidator


def main():
    """Run the complete data collection pipeline."""
    logger.info("=" * 80)
    logger.info("Starting Lead Sniper Data Collection Pipeline")
    logger.info("=" * 80)

    try:
        # ===== Step 1: Load manually verified companies with real financials =====
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Loading manually verified companies with financials...")
        logger.info("=" * 80)

        raw_file = Path(__file__).parent.parent / "data" / "raw" / "manual_seed_with_financials.json"
        with open(raw_file, 'r', encoding='utf-8') as f:
            companies = json.load(f)

        logger.success(f"Loaded {len(companies)} manually verified companies")

        # ===== Step 2: Clean and normalize data =====
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Cleaning and normalizing data...")
        logger.info("=" * 80)

        cleaner = DataCleaner()
        df = cleaner.clean_companies(companies)
        df = cleaner.ensure_required_columns(df)
        logger.success(f"Data cleaned: {len(df)} records")

        # ===== Step 3: Deduplicate =====
        deduplicator = Deduplicator()
        df = deduplicator.deduplicate(df)
        logger.success(f"Data deduplicated: {len(df)} unique records")

        # ===== Step 4: Validate and filter =====
        validator = DataValidator(min_revenue=MIN_REVENUE)
        df = validator.validate(df)
        df = validator.check_relevance_by_okved(df)
        logger.success(f"Data validated: {len(df)} records passed filters")

        # ===== Step 5: Export final CSV =====
        logger.info("\n" + "=" * 80)
        logger.info("STEP 5: Exporting final CSV...")
        logger.info("=" * 80)

        column_order = [
            'inn', 'name', 'revenue_year', 'revenue', 'segment_tag', 'source',
            'okved_main', 'employees', 'site', 'description', 'region',
            'contacts', 'rating_ref', 'okved_relevant'
        ]
        final_columns = [col for col in column_order if col in df.columns]
        df_export = df[final_columns]

        if 'revenue' in df_export.columns:
            df_export = df_export.sort_values('revenue', ascending=False)

        df_export.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        logger.success("=" * 80)
        logger.success(f"Pipeline completed successfully!")
        logger.success(f"Total companies: {len(df_export)}")
        logger.success(f"Output file: {OUTPUT_FILE}")
        logger.success("=" * 80)

        logger.info("\nTop 5 companies by revenue:")
        for _, row in df_export.head().iterrows():
            logger.info(f"  {row['name']} - {row['revenue']:,} ₽ ({row['segment_tag']})")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        raise


if __name__ == "__main__":
    main()