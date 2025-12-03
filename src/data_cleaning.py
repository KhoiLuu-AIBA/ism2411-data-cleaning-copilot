import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data(filepath):
    """Load raw data with error handling."""
    try:
        # Ensure we're only reading CSV files
        if not str(filepath).endswith('.csv'):
            logger.error(f"File is not a CSV: {filepath}")
            raise ValueError("Only CSV files are supported")
            
        # Check if file exists
        if not os.path.exists(filepath):
            logger.error(f"File not found: {filepath}")
            raise FileNotFoundError(f"File not found: {filepath}")
            
        df = pd.read_csv(filepath)
        logger.info(f"Loaded {len(df)} rows from {filepath}")
        return df
    except Exception as e:
        logger.error(f"Error loading data from {filepath}: {e}")
        raise

def clean_column_names(df):
    """Standardize column names and clean text columns."""
    logger.info("Standardizing column names and cleaning text...")
    
    # Standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    # Clean product names and categories
    if 'prodname' in df.columns:
        df['prodname'] = df['prodname'].astype(str).str.strip().str.lower()
    if 'category' in df.columns:
        df['category'] = df['category'].astype(str).str.strip().str.lower()
    
    logger.info(f"Column cleaning complete. {len(df)} rows")
    return df

def handle_missing_values(df):
    """Handle missing values in numeric columns and convert data types."""
    logger.info("Handling missing values...")
    
    # Convert empty strings to NaN
    df = df.replace(r'^\s*$', np.nan, regex=True)
    
    # Convert numeric columns
    if 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
    if 'qty' in df.columns:
        df['qty'] = pd.to_numeric(df['qty'], errors='coerce')
    
    # Remove rows where all critical columns are missing
    critical_cols = ['price', 'qty', 'date_sold']
    existing_critical_cols = [col for col in critical_cols if col in df.columns]
    df = df.dropna(subset=existing_critical_cols, how='all')
    
    logger.info(f"Missing values handled. {len(df)} rows")
    return df

def remove_invalid_rows(df):
    """Remove rows with invalid values."""
    logger.info("Removing invalid rows...")
    
    # Remove rows with negative quantities
    if 'qty' in df.columns:
        initial_count = len(df)
        df = df[df['qty'] >= 0]
        removed = initial_count - len(df)
        logger.info(f"Removed {removed} rows with negative quantities")
    
    # Remove rows with negative prices
    if 'price' in df.columns:
        initial_count = len(df)
        df = df[df['price'] >= 0]
        removed = initial_count - len(df)
        logger.info(f"Removed {removed} rows with negative prices")
    
    # Remove rows without sale dates
    if 'date_sold' in df.columns:
        initial_count = len(df)
        df = df.dropna(subset=['date_sold'])
        removed = initial_count - len(df)
        logger.info(f"Removed {removed} rows without sale dates")
    
    logger.info(f"Invalid rows removed. {len(df)} rows remaining")
    return df

if __name__ == "__main__":
    raw_path = "data/raw/sales_data_raw.csv"
    cleaned_path = "data/processed/sales_data_clean.csv"

    df_raw = load_data(raw_path)
    df_clean = clean_column_names(df_raw)
    df_clean = handle_missing_values(df_clean)
    df_clean = remove_invalid_rows(df_clean)
    
    # Ensure the output directory exists
    Path(cleaned_path).parent.mkdir(parents=True, exist_ok=True)
    
    df_clean.to_csv(cleaned_path, index=False)
    print("Cleaning complete. First few rows:")
    print(df_clean.head())