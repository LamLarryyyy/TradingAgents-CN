"""
US stock data source adapter using Alpha Vantage
Note: This adapter does NOT inherit from DataSourceAdapter to avoid abstract method requirements.
It's a standalone adapter specifically for US stock sync.
"""
from typing import Optional, Dict, List
import logging
from datetime import datetime, timedelta
import pandas as pd
import os
import io

logger = logging.getLogger(__name__)


class USAlphaVantageAdapter:
    """美股數據源適配器 (使用 Alpha Vantage)"""

    def __init__(self, api_key: str = None):
        self._api_key = api_key

    @property
    def name(self) -> str:
        return "alphavantage_us"

    @property
    def api_key(self) -> str:
        """獲取 API Key"""
        if self._api_key:
            return self._api_key
        
        # 從環境變量獲取
        env_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        if env_key:
            return env_key
        
        # 從數據庫獲取
        try:
            from app.core.database import get_mongo_db_sync
            db = get_mongo_db_sync()
            for cfg in db.system_configs.find().sort('updated_at', -1).limit(1):
                if 'data_source_configs' in cfg:
                    for ds in cfg['data_source_configs']:
                        if ds.get('type') == 'alpha_vantage' and ds.get('api_key'):
                            return ds.get('api_key')
        except Exception as e:
            logger.warning(f"Failed to get Alpha Vantage API key from database: {e}")
        
        return None

    def _get_default_priority(self) -> int:
        return 2

    def is_available(self) -> bool:
        """檢查 Alpha Vantage 是否可用"""
        return bool(self.api_key)

    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """獲取美股股票列表"""
        if not self.is_available():
            logger.warning("Alpha Vantage: API key not configured")
            return None
        try:
            import requests
            logger.info("Alpha Vantage: Fetching US stock listing...")

            url = 'https://www.alphavantage.co/query'
            params = {
                'function': 'LISTING_STATUS',
                'apikey': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=60)
            
            if response.status_code != 200:
                logger.error(f"Alpha Vantage: HTTP {response.status_code}")
                return None

            # 解析 CSV 響應
            df = pd.read_csv(io.StringIO(response.text))
            
            if df is None or df.empty:
                logger.warning("Alpha Vantage: LISTING_STATUS returned empty data")
                return None

            # 只保留活躍的股票
            if 'status' in df.columns:
                df = df[df['status'] == 'Active']

            # 只保留股票類型
            if 'assetType' in df.columns:
                df = df[df['assetType'] == 'Stock']

            # 標準化列名
            df = df.rename(columns={
                'symbol': 'symbol',
                'name': 'name',
                'exchange': 'exchange',
                'assetType': 'asset_type',
                'ipoDate': 'ipo_date',
                'delistingDate': 'delisting_date',
                'status': 'status',
            })

            # 添加市場標識
            df['market'] = 'US'
            df['ts_code'] = df['symbol'].apply(lambda x: f"{x}.US")

            logger.info(f"Alpha Vantage: Successfully fetched {len(df)} US stocks")
            return df

        except Exception as e:
            logger.error(f"Alpha Vantage: Failed to fetch stock list: {e}")
            return None

    def get_daily_data(self, symbol: str, start_date: str = None, end_date: str = None, outputsize: str = "compact") -> Optional[pd.DataFrame]:
        """獲取美股日線數據
        
        Args:
            symbol: 股票代碼 (如 'AAPL')
            start_date: 開始日期 (YYYY-MM-DD)
            end_date: 結束日期 (YYYY-MM-DD)
            outputsize: 'compact' (最近100天) 或 'full' (完整歷史)
        """
        if not self.is_available():
            return None
        try:
            import requests
            
            # 移除可能的 .US 後綴
            symbol = symbol.replace('.US', '').replace('.us', '')
            
            url = 'https://www.alphavantage.co/query'
            params = {
                'function': 'TIME_SERIES_DAILY_ADJUSTED',
                'symbol': symbol,
                'outputsize': outputsize,
                'apikey': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            
            # 檢查錯誤
            if 'Error Message' in data:
                logger.error(f"Alpha Vantage: {data['Error Message']}")
                return None
            if 'Note' in data:
                logger.warning(f"Alpha Vantage: Rate limit - {data['Note']}")
                return None
            if 'Information' in data:
                logger.warning(f"Alpha Vantage: {data['Information']}")
                return None
            
            time_series = data.get('Time Series (Daily)', {})
            if not time_series:
                return None

            # 轉換為 DataFrame
            records = []
            for date_str, values in time_series.items():
                records.append({
                    'trade_date': date_str.replace('-', ''),
                    'open': float(values.get('1. open', 0)),
                    'high': float(values.get('2. high', 0)),
                    'low': float(values.get('3. low', 0)),
                    'close': float(values.get('4. close', 0)),
                    'adj_close': float(values.get('5. adjusted close', 0)),
                    'vol': int(values.get('6. volume', 0)),
                    'dividend': float(values.get('7. dividend amount', 0)),
                    'split_coef': float(values.get('8. split coefficient', 1)),
                })
            
            df = pd.DataFrame(records)
            df = df.sort_values('trade_date')

            # 過濾日期範圍
            if start_date:
                start_date_fmt = start_date.replace('-', '')
                df = df[df['trade_date'] >= start_date_fmt]
            if end_date:
                end_date_fmt = end_date.replace('-', '')
                df = df[df['trade_date'] <= end_date_fmt]

            df['symbol'] = symbol
            df['market'] = 'US'

            return df

        except Exception as e:
            logger.error(f"Alpha Vantage: Failed to fetch daily data for {symbol}: {e}")
            return None

    def get_stock_info(self, symbol: str) -> Optional[Dict]:
        """獲取美股基本信息"""
        if not self.is_available():
            return None
        try:
            import requests
            
            symbol = symbol.replace('.US', '').replace('.us', '')
            
            url = 'https://www.alphavantage.co/query'
            params = {
                'function': 'OVERVIEW',
                'symbol': symbol,
                'apikey': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            
            if 'Error Message' in data or 'Note' in data or not data:
                return {"symbol": symbol, "market": "US"}
            
            return {
                'symbol': data.get('Symbol', symbol),
                'name': data.get('Name', ''),
                'description': data.get('Description', ''),
                'exchange': data.get('Exchange', ''),
                'currency': data.get('Currency', 'USD'),
                'country': data.get('Country', 'USA'),
                'sector': data.get('Sector', ''),
                'industry': data.get('Industry', ''),
                'market_cap': data.get('MarketCapitalization', ''),
                'pe_ratio': data.get('PERatio', ''),
                'pb_ratio': data.get('PriceToBookRatio', ''),
                'dividend_yield': data.get('DividendYield', ''),
                'eps': data.get('EPS', ''),
                'beta': data.get('Beta', ''),
                '52_week_high': data.get('52WeekHigh', ''),
                '52_week_low': data.get('52WeekLow', ''),
                'market': 'US',
            }

        except Exception as e:
            logger.error(f"Alpha Vantage: Failed to fetch stock info for {symbol}: {e}")
            return None

    def get_quote(self, symbol: str) -> Optional[Dict]:
        """獲取美股實時報價"""
        if not self.is_available():
            return None
        try:
            import requests
            
            symbol = symbol.replace('.US', '').replace('.us', '')
            
            url = 'https://www.alphavantage.co/query'
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            
            quote = data.get('Global Quote', {})
            if not quote:
                return None
            
            return {
                'symbol': quote.get('01. symbol', symbol),
                'open': float(quote.get('02. open', 0)),
                'high': float(quote.get('03. high', 0)),
                'low': float(quote.get('04. low', 0)),
                'price': float(quote.get('05. price', 0)),
                'volume': int(quote.get('06. volume', 0)),
                'latest_trading_day': quote.get('07. latest trading day', ''),
                'previous_close': float(quote.get('08. previous close', 0)),
                'change': float(quote.get('09. change', 0)),
                'change_percent': quote.get('10. change percent', '').replace('%', ''),
                'market': 'US',
            }

        except Exception as e:
            logger.error(f"Alpha Vantage: Failed to fetch quote for {symbol}: {e}")
            return None
