"""
Hong Kong stock data source adapter using AKShare
Note: This adapter does NOT inherit from DataSourceAdapter to avoid abstract method requirements.
It's a standalone adapter specifically for HK stock sync.
"""
from typing import Optional, Dict, List
import logging
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger(__name__)


class HKAKShareAdapter:
    """港股數據源適配器 (使用 AKShare)"""

    def __init__(self):
        pass

    @property
    def name(self) -> str:
        return "akshare_hk"

    def _get_default_priority(self) -> int:
        return 2

    def is_available(self) -> bool:
        """檢查 AKShare 是否可用"""
        try:
            import akshare as ak  # noqa: F401
            return True
        except ImportError:
            return False

    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """獲取港股股票列表"""
        if not self.is_available():
            return None
        try:
            import akshare as ak
            logger.info("AKShare HK: Fetching Hong Kong stock list...")

            # 使用港股實時行情接口獲取股票列表
            df = ak.stock_hk_spot_em()

            if df is None or df.empty:
                logger.warning("AKShare HK: stock_hk_spot_em() returned empty data")
                return None

            # 標準化列名
            df = df.rename(columns={
                '代码': 'symbol',
                '名称': 'name',
                '最新价': 'price',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '最高': 'high',
                '最低': 'low',
                '今开': 'open',
                '昨收': 'pre_close',
                '换手率': 'turnover_rate',
            })

            # 確保有必需的列
            if 'symbol' not in df.columns or 'name' not in df.columns:
                logger.error(f"AKShare HK: Unexpected column names: {df.columns.tolist()}")
                return None

            # 添加市場標識
            df['market'] = 'HK'
            df['exchange'] = 'HKEX'
            
            # 生成標準代碼格式
            df['ts_code'] = df['symbol'].apply(lambda x: f"{x}.HK")

            logger.info(f"AKShare HK: Successfully fetched {len(df)} Hong Kong stocks")
            return df

        except Exception as e:
            logger.error(f"AKShare HK: Failed to fetch stock list: {e}")
            return None

    def get_daily_data(self, symbol: str, start_date: str = None, end_date: str = None, adjust: str = "qfq") -> Optional[pd.DataFrame]:
        """獲取港股日線數據
        
        Args:
            symbol: 股票代碼 (如 '00700')
            start_date: 開始日期 (YYYY-MM-DD)
            end_date: 結束日期 (YYYY-MM-DD)
            adjust: 復權類型 ('qfq'=前復權, 'hfq'=後復權, ''=不復權)
        """
        if not self.is_available():
            return None
        try:
            import akshare as ak
            
            # 移除可能的 .HK 後綴
            symbol = symbol.replace('.HK', '').replace('.hk', '')
            
            df = ak.stock_hk_daily(symbol=symbol, adjust=adjust)
            
            if df is None or df.empty:
                return None

            # 標準化列名
            df = df.rename(columns={
                'date': 'trade_date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'vol',
            })

            # 過濾日期範圍
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                if start_date:
                    df = df[df['trade_date'] >= pd.to_datetime(start_date)]
                if end_date:
                    df = df[df['trade_date'] <= pd.to_datetime(end_date)]
                df['trade_date'] = df['trade_date'].dt.strftime('%Y%m%d')

            df['symbol'] = symbol
            df['market'] = 'HK'

            return df

        except Exception as e:
            logger.error(f"AKShare HK: Failed to fetch daily data for {symbol}: {e}")
            return None

    def get_stock_info(self, symbol: str) -> Optional[Dict]:
        """獲取港股基本信息"""
        if not self.is_available():
            return None
        try:
            import akshare as ak
            
            symbol = symbol.replace('.HK', '').replace('.hk', '')
            
            # 嘗試獲取公司資料
            try:
                df = ak.stock_hk_company_profile_em(symbol=symbol)
                if df is not None and not df.empty:
                    # 轉換為字典
                    info = {}
                    for _, row in df.iterrows():
                        key = row.get('item', row.get('项目', ''))
                        value = row.get('value', row.get('值', ''))
                        if key:
                            info[key] = value
                    return info
            except Exception:
                pass
            
            return {"symbol": symbol, "market": "HK"}

        except Exception as e:
            logger.error(f"AKShare HK: Failed to fetch stock info for {symbol}: {e}")
            return None
