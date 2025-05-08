import logging
import sqlite3
from datetime import datetime
from typing import Literal, Optional

import pandas as pd
from prefect import task, flow, get_run_logger

import yfinance_utils

logger = logging.getLogger(__name__)


def create_snapshot(df: pd.DataFrame, underlyingPrice: float) -> pd.DataFrame:
    """Select 30 option contracts daily using three-bucket strategy:
    1. Top 10 by volume (with OI > 20)
    2. Top 10 by OI (excluding bucket 1 selections)
    3. Top 10 by remaining volume and near-the-money options
    
    Args:
        df: DataFrame containing option contract data with columns:
            - volume: trading volume
            - openInterest: open interest
            - strike: strike price
            - expiryDate: expiration date
            - optionType: 'C' or 'P'
            - lastPrice: last traded price
    
    Returns:
        DataFrame with selected 30 contracts
    """

    # First filter out low liquidity contracts
    df = df[(df['openInterest'] >= 20) & (df['volume'] >= 10)].copy()
    
    # Bucket 1: Top 10 by volume
    bucket1 = df.nlargest(10, 'volume')
    remaining = df[~df.index.isin(bucket1.index)]
    
    # Bucket 2: Top 10 by OI from remaining
    bucket2 = remaining.nlargest(10, 'openInterest')
    remaining = remaining[~remaining.index.isin(bucket2.index)]
    
    # Bucket 3: 10 near-the-money options
    # Get current price
    current_price = underlyingPrice
    
    # Calculate moneyness (absolute distance from current price)
    remaining['moneyness'] = abs(remaining['strike'] - current_price)
    
    # Select 10 nearest-to-money options
    # Ensure balanced selection of calls and puts (5 each)
    calls = remaining[remaining['optionType'] == 'C']
    puts = remaining[remaining['optionType'] == 'P']
    
    near_money_calls = calls.sort_values(['moneyness', 'expiryDate']).head(5)
    near_money_puts = puts.sort_values(['moneyness', 'expiryDate']).head(5)
    
    bucket3 = pd.concat([near_money_calls, near_money_puts])
    
    # Drop moneyness column
    bucket3 = bucket3.drop('moneyness', axis=1)

    # Combine all buckets and return
    return pd.concat([bucket1, bucket2, bucket3]).reset_index(drop=True)


def get_options(
    ticker_symbol: str,
    num_options: int = 10,
    start_date: str | None = None,
    end_date: str | None = None,
    strike_lower: float | None = None,
    strike_upper: float | None = None,
    option_type: Literal["C", "P"] | None = None,
) -> pd.DataFrame:
    """Get options with bucketed selection. Dates: YYYY-MM-DD. Type: C=calls, P=puts."""
    underlyingPrice = yfinance_utils.get_current_price(ticker_symbol)
    logger.info(f"Current stock price for {ticker_symbol}: {underlyingPrice}")


    try:
        df, error = yfinance_utils.get_filtered_options(
            ticker_symbol, start_date, end_date, strike_lower, strike_upper, option_type
        )

        if error:
            return error


        if len(df) == 0:
            return f"No options found for {ticker_symbol}"

        # pick up some of the columns
        df = df[["contractSymbol", "strike", "lastPrice", "lastTradeDate", "change", "volume", "openInterest", "impliedVolatility", "expiryDate"]]
        # add new columns, ticker symbol , snapshot date and underlying price
        df["tickerSymbol"] = ticker_symbol
        df["snapshotDate"] = datetime.now().strftime("%Y-%m-%d")
        df["underlyingPrice"] = underlyingPrice
        df["optionType"] = df["contractSymbol"].apply(lambda x: "C" if "C" in x else "P")
        

        return create_snapshot(df, underlyingPrice)
    except Exception as e:
        logger.error(f"Error getting options data for {ticker_symbol}: {e}")
        return f"Failed to retrieve options data for {ticker_symbol}: {str(e)}"


@task(name="get-options-data", retries=2, retry_delay_seconds=60)
def get_options_task(
    ticker_symbol: str,
    num_options: int = 10,
    start_date: str | None = None,
    end_date: str | None = None,
    strike_lower: float | None = None,
    strike_upper: float | None = None,
    option_type: Literal["C", "P"] | None = None,
) -> pd.DataFrame:
    """Prefect task to get options with highest open interest.
    
    Args:
        ticker_symbol: Stock ticker symbol
        num_options: Number of options to return (default: 10)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        strike_lower: Minimum strike price
        strike_upper: Maximum strike price
        option_type: 'C' for calls or 'P' for puts
    
    Returns:
        DataFrame with options data sorted by open interest
    """
    logger = get_run_logger()
    try:
        return get_options(
            ticker_symbol,
            num_options,
            start_date,
            end_date,
            strike_lower,
            strike_upper,
            option_type
        )
    except Exception as e:
        logger.error(f"Task failed for {ticker_symbol}: {str(e)}")
        raise

@task(name="validate-options-data")
def validate_options_task(options_df: pd.DataFrame) -> bool:
    """Validate options data meets expected schema and quality.
    
    Args:
        options_df: DataFrame containing options data
        
    Returns:
        bool: True if validation passes, False otherwise
    """
    logger = get_run_logger()
    required_columns = {
        "optionType": str,
        "tickerSymbol": str,
        "snapshotDate": str,
        "contractSymbol": str,
        "strike": float,
        "lastPrice": float,
        "lastTradeDate": datetime,
        "change": float,
        "volume": int,
        "openInterest": int,
        "impliedVolatility": float,
        "expiryDate": datetime,
    }
    
    if not isinstance(options_df, pd.DataFrame):
        logger.error("Input is not a pandas DataFrame")
        logger.error(f"Input type: {type(options_df)}")
        logger.error(f"Input value: {options_df}")
        return False
        
    missing_cols = set(required_columns.keys()) - set(options_df.columns)
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        return False
        
    return True

@task(name="save-options-data")
def save_options_task(
    options_df: pd.DataFrame,
    db_path: str = "options_data.db",
    table_name: str = "options"
) -> bool:
    """Save options data to SQLite database.
    
    Args:
        options_df: DataFrame containing options data
        db_path: Path to SQLite database file
        table_name: Name of table to store data in
        
    Returns:
        bool: True if save succeeded, False otherwise
    """
    logger = get_run_logger()
    
    try:
        conn = sqlite3.connect(db_path)
        options_df.to_sql(
            name=table_name,
            con=conn,
            if_exists='append',
            index=False
        )
        conn.close()
        logger.info(f"Successfully saved {len(options_df)} records to {db_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save options data: {str(e)}")
        return False
    

if __name__ == "__main__":
    get_options("AAPL")