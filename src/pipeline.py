from datetime import datetime
from typing import List
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner

import yfinance_task 
@flow(task_runner=ConcurrentTaskRunner())
def stock_data_pipeline(tickers: List[str]=["AAPL"]):
    """Main data pipeline that gets, validates and saves options data"""
    logger = get_run_logger()
    
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
        else:
            logger.error(f"Failed to save data for {ticker}")

if __name__ == "__main__":
    stock_data_pipeline(["AAPL"])
