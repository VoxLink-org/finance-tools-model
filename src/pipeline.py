from datetime import datetime
from typing import List
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from pyrate_limiter import Duration, RequestRate, Limiter
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
import yfinance as yf
import pandas as pd
from curl_cffi import Session as CurlSession
import os

class ChromeSession(CurlSession):
    def __init__(self, **kwargs):
        kwargs['impersonate'] = "chrome"
        super().__init__(**kwargs)

class RateLimitedSession(LimiterMixin, ChromeSession):
    pass

# 全局session配置
session = RateLimitedSession(
    limiter=Limiter(RequestRate(5, Duration.SECOND)),
    bucket_class=MemoryQueueBucket
)

@task(retries=2, retry_delay_seconds=10)
def fetch_valuation(ticker: str, period: str = "3y"):
    """获取单个股票的估值数据"""
    logger = get_run_logger()
    try:
        stock = yf.Ticker(ticker, session=session)
        hist = stock.history(period=period)
        valuation = {
            'pe_ratio': stock.info.get('trailingPE'),
            'pb_ratio': stock.info.get('priceToBook'),
            'ps_ratio': stock.info.get('priceToSalesTrailing12Months')
        }
        return {'ticker': ticker, 'history': hist, 'valuation': valuation}
    except Exception as e:
        logger.error(f"Failed to fetch valuation for {ticker}: {str(e)}")
        raise

@task(retries=2, retry_delay_seconds=10)
def fetch_industry(ticker: str):
    """获取单个股票的行业信息"""
    logger = get_run_logger()
    try:
        stock = yf.Ticker(ticker, session=session)
        info = stock.info
        return {
            'ticker': ticker,
            'sector': info.get('sector'),
            'industry': info.get('industry'),
            'country': info.get('country')
        }
    except Exception as e:
        logger.error(f"Failed to fetch industry info for {ticker}: {str(e)}")
        raise

@task(retries=2, retry_delay_seconds=10)
def fetch_financials(ticker: str):
    """获取单个股票的财报数据"""
    logger = get_run_logger()
    try:
        stock = yf.Ticker(ticker, session=session)
        return {
            'ticker': ticker,
            'income_stmt': stock.financials,
            'balance_sheet': stock.balance_sheet,
            'cash_flow': stock.cashflow,
            'earnings_dates': stock.get_earnings_dates()
        }
    except Exception as e:
        logger.error(f"Failed to fetch financials for {ticker}: {str(e)}")
        raise

@task
def save_data(ticker: str, valuation: dict, industry: dict, financials: dict):
    """保存单个股票的数据到不同文件"""
    data_dir = "data/processed"
    os.makedirs(data_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save valuation data
    valuation_data = {'valuation': valuation, 'last_updated': timestamp}
    valuation_path = os.path.join(data_dir, f"{ticker}_valuation.csv")
    pd.DataFrame.from_dict(valuation_data).to_csv(valuation_path)
    
    # Save industry data
    industry_data = {'industry': industry, 'last_updated': timestamp}
    industry_path = os.path.join(data_dir, f"{ticker}_industry.csv")
    pd.DataFrame.from_dict(industry_data).to_csv(industry_path)
    
    # Save financials data
    financials_data = {'financials': financials, 'last_updated': timestamp}
    financials_path = os.path.join(data_dir, f"{ticker}_financials.csv")
    pd.DataFrame.from_dict(financials_data).to_csv(financials_path)
    
    return [valuation_path, industry_path, financials_path]

@flow(task_runner=ConcurrentTaskRunner())
def stock_data_pipeline(tickers: List[str]=["AAPL", "MSFT", "GOOG"]):
    """主数据管道"""
    for ticker in tickers:
        # 并行获取数据
        valuation = fetch_valuation.submit(ticker, period="1y")
        industry = fetch_industry.submit(ticker)
        financials = fetch_financials.submit(ticker)
        
        # 等待所有数据就绪后保存
        save_data.submit(
            ticker,
            valuation.result(),
            industry.result(), 
            financials.result()
        )

if __name__ == "__main__":
    stock_data_pipeline(["AAPL", "MSFT", "GOOG"])
