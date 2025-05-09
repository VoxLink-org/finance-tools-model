from prefect.server.schemas.schedules import IntervalSchedule
import datetime
from pipeline import stock_data_pipeline

if __name__ == "__main__":
    stock_data_pipeline.deploy(
        name="stock-data-pipeline-all",
        schedule=IntervalSchedule(
            interval=datetime.timedelta(days=1),
            anchor_date=datetime.datetime.now().replace(hour=17, minute=0, second=0, microsecond=0),
            timezone="America/New_York"
        ),
        tags=["production", "yfinance"],
        work_pool_name="default-pool"
    )
