import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data(filepath):
    """Load raw data with error handling."""
    try:
        df = pd.read_csv(filepath)
        logger.info(f"Loaded {len(df)} rows from {filepath}")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise

def clean_column_names(df): #Apply all data cleaning transformations
    logger.info("Starting data cleaning process...")
    
    # Standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    # Clean text columns
    text_columns = ['prodname', 'category']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()
    
    # Convert empty strings to NaN
    df = df.replace(r'^\s*$', np.nan, regex=True)
    
    # Convert numeric columns
    numeric_columns = {'price': 'Price', 'qty': 'Quantity'}
    for col, name in numeric_columns.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove rows with all missing values in critical columns
    critical_cols = ['price', 'qty', 'date_sold']
    existing_critical_cols = [col for col in critical_cols if col in df.columns]
    df = df.dropna(subset=existing_critical_cols, how='all')
    
    # Validate numeric constraints
    if 'qty' in df.columns:
        df = df[df['qty'] >= 0]
    if 'price' in df.columns:
        df = df[df['price'] >= 0]
    
    logger.info(f"Cleaning complete. {len(df)} rows remaining")
    return df

def remove_invalid_rows(df, filepath): #Save cleaned data with error handling
    try:
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Saved cleaned data to {filepath}")
        return df
    except Exception as e:
        logger.error(f"Failed to save data: {e}")
        raise

if __name__ == '__main__':
    raw_path = "data/raw/sales_data_raw.csv"
    cleaned_path = "data/processed/sales_data_clean.csv"
    df_raw = load_data(raw_path)
    df_clean = clean_column_names(df_raw)
    df_clean = remove_invalid_rows(df_clean, cleaned_path)
    df_clean.to_csv(cleaned_path, index=False)
    print("Data cleaning process completed successfully. \nCleaned data preview:")
    print(load_data(cleaned_path))