from datetime import datetime
from typing import List
import get_ticker_pool
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.artifacts import create_link_artifact

import yfinance_task 
@flow(task_runner=ConcurrentTaskRunner())
def stock_data_pipeline(tickers: List[str]):
    """Main data pipeline that gets, validates and saves options data"""
    logger = get_run_logger()
    
        
    if not tickers or len(tickers) == 0:
        logger.error("No tickers provided")
        return
        
    for ticker in tickers:
        logger.info(f"Processing ticker: {ticker}")
        
        # Get options data
        options_data = yfinance_task.get_options_task(ticker)
        
        # Validate data
        is_valid = yfinance_task.validate_options_task(options_data)
        
        if not is_valid:
            logger.error(f"Invalid options data for {ticker}")
            continue
            
        # Save valid data
        save_success = yfinance_task.save_options_task(options_data)
        
        if save_success:
            logger.info(f"Successfully processed {ticker}")
            
            # Clean up old data
            deleted_count = yfinance_task.clean_up_the_days_before_10days()
            logger.info(f"Cleaned up {deleted_count} old records for {ticker}")
        else:
            logger.error(f"Failed to save data for {ticker}")

    create_link_artifact(
        key="options-data",
        link="https://prefect.findata-be.uk/link_artifact/options_data.db",
        description="## Highly variable data",
    )

def main():
    stock_data_pipeline(get_ticker_pool.get_most_active_tickers_from_tradingview()[:5])



if __name__ == "__main__":
    stock_data_pipeline(["PLTR"])
