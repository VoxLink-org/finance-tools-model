from datetime import  datetime
import get_ticker_pool
from prefect.schedules import Cron
import datetime
from pipeline import stock_data_pipeline

if __name__ == "__main__":
    tickers =get_ticker_pool.get_most_active_tickers_from_tradingview()
    stock_data_pipeline.serve(
        name="stock-data-pipeline-all",
        schedule=Cron(
            "0 17 * * 1,2,3,4,5",
            timezone="America/New_York"
        ),
        parameters={"tickers": tickers}
    )
