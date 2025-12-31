"""
Foreign stock (HK & US) data synchronization service
- HK stocks: AKShare
- US stocks: Alpha Vantage
- Syncs stock basic info into MongoDB collection `stock_basic_info`
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne

from app.core.database import get_mongo_db

logger = logging.getLogger(__name__)

# Collection names
COLLECTION_NAME = "stock_basic_info"
STATUS_COLLECTION = "sync_status"


class MarketType(Enum):
    """市場類型"""
    HK = "hk_stocks"
    US = "us_stocks"


@dataclass
class ForeignSyncStats:
    """同步統計信息"""
    job: str = ""
    data_type: str = "stock_basics"
    market: str = ""
    status: str = "idle"
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    total: int = 0
    inserted: int = 0
    updated: int = 0
    errors: int = 0
    data_source: str = ""
    message: Optional[str] = None


class ForeignStockSyncService:
    """港股和美股數據同步服務"""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._running = False
        self._last_status: Dict[str, Any] = {}

    async def get_status(self, market: str) -> Dict[str, Any]:
        """獲取同步狀態"""
        job_key = f"stock_basics_{market.lower()}"
        
        if job_key in self._last_status:
            return self._last_status[job_key]

        db = get_mongo_db()
        doc = await db[STATUS_COLLECTION].find_one({
            "job": job_key,
            "data_type": "stock_basics"
        })
        if doc:
            doc.pop("_id", None)
            return doc
        return {"job": job_key, "status": "never_run", "market": market}

    async def _persist_status(self, db: AsyncIOMotorDatabase, stats: Dict[str, Any]) -> None:
        """持久化同步狀態"""
        filter_query = {
            "data_type": stats.get("data_type", "stock_basics"),
            "job": stats.get("job")
        }

        await db[STATUS_COLLECTION].update_one(
            filter_query,
            {"$set": stats},
            upsert=True
        )

        self._last_status[stats.get("job")] = {k: v for k, v in stats.items() if k != "_id"}

    async def sync_hk_stocks(self, force: bool = False) -> Dict[str, Any]:
        """同步港股數據 (使用 AKShare)"""
        job_key = "stock_basics_hk"
        market = "HK"
        
        if self._running:
            return {"status": "already_running", "message": "同步任務正在執行中"}

        async with self._lock:
            self._running = True
            db = get_mongo_db()
            
            stats = ForeignSyncStats(
                job=job_key,
                market=market,
                status="running",
                started_at=datetime.now().isoformat(),
                data_source="akshare"
            )
            
            await self._persist_status(db, stats.__dict__)

            try:
                from app.services.data_sources.hk_akshare_adapter import HKAKShareAdapter
                
                adapter = HKAKShareAdapter()
                if not adapter.is_available():
                    stats.status = "failed"
                    stats.message = "AKShare 不可用"
                    stats.finished_at = datetime.now().isoformat()
                    await self._persist_status(db, stats.__dict__)
                    return stats.__dict__

                logger.info("🔄 開始同步港股數據...")
                
                # 獲取股票列表
                df = adapter.get_stock_list()
                if df is None or df.empty:
                    stats.status = "failed"
                    stats.message = "無法獲取港股列表"
                    stats.finished_at = datetime.now().isoformat()
                    await self._persist_status(db, stats.__dict__)
                    return stats.__dict__

                stats.total = len(df)
                logger.info(f"📊 獲取到 {stats.total} 支港股")

                # 批量更新到 MongoDB
                operations = []
                for _, row in df.iterrows():
                    symbol = str(row.get('symbol', '')).strip()
                    if not symbol:
                        continue

                    doc = {
                        "code": symbol,
                        "ts_code": f"{symbol}.HK",
                        "name": row.get('name', ''),
                        "market": "HK",
                        "exchange": "HKEX",
                        "category": "stock_hk",
                        "market_info": {
                            "market": "HK",
                            "exchange": "HKEX",
                        },
                        "price": row.get('price'),
                        "pct_change": row.get('pct_change'),
                        "volume": row.get('volume'),
                        "amount": row.get('amount'),
                        "updated_at": datetime.now(),
                        "data_source": "akshare",
                    }

                    operations.append(
                        UpdateOne(
                            {"code": symbol, "market": "HK"},
                            {"$set": doc, "$setOnInsert": {"created_at": datetime.now()}},
                            upsert=True
                        )
                    )

                # 執行批量寫入
                if operations:
                    result = await db[COLLECTION_NAME].bulk_write(operations, ordered=False)
                    stats.inserted = result.upserted_count
                    stats.updated = result.modified_count
                    logger.info(f"✅ 港股同步完成: 新增 {stats.inserted}, 更新 {stats.updated}")

                stats.status = "completed"
                stats.message = f"成功同步 {stats.total} 支港股"
                stats.finished_at = datetime.now().isoformat()

            except Exception as e:
                logger.error(f"❌ 港股同步失敗: {e}", exc_info=True)
                stats.status = "failed"
                stats.message = str(e)
                stats.errors = 1
                stats.finished_at = datetime.now().isoformat()

            finally:
                self._running = False
                await self._persist_status(db, stats.__dict__)

            return stats.__dict__

    async def sync_us_stocks(self, force: bool = False) -> Dict[str, Any]:
        """同步美股數據 (使用 Alpha Vantage)"""
        job_key = "stock_basics_us"
        market = "US"
        
        if self._running:
            return {"status": "already_running", "message": "同步任務正在執行中"}

        async with self._lock:
            self._running = True
            db = get_mongo_db()
            
            stats = ForeignSyncStats(
                job=job_key,
                market=market,
                status="running",
                started_at=datetime.now().isoformat(),
                data_source="alphavantage"
            )
            
            await self._persist_status(db, stats.__dict__)

            try:
                from app.services.data_sources.us_alphavantage_adapter import USAlphaVantageAdapter
                
                adapter = USAlphaVantageAdapter()
                if not adapter.is_available():
                    stats.status = "failed"
                    stats.message = "Alpha Vantage API Key 未配置"
                    stats.finished_at = datetime.now().isoformat()
                    await self._persist_status(db, stats.__dict__)
                    return stats.__dict__

                logger.info("🔄 開始同步美股數據...")
                
                # 獲取股票列表
                df = adapter.get_stock_list()
                if df is None or df.empty:
                    stats.status = "failed"
                    stats.message = "無法獲取美股列表"
                    stats.finished_at = datetime.now().isoformat()
                    await self._persist_status(db, stats.__dict__)
                    return stats.__dict__

                stats.total = len(df)
                logger.info(f"📊 獲取到 {stats.total} 支美股")

                # 批量更新到 MongoDB
                operations = []
                for _, row in df.iterrows():
                    symbol = str(row.get('symbol', '')).strip()
                    if not symbol:
                        continue

                    doc = {
                        "code": symbol,
                        "ts_code": f"{symbol}.US",
                        "name": row.get('name', ''),
                        "market": "US",
                        "exchange": row.get('exchange', ''),
                        "category": "stock_us",
                        "market_info": {
                            "market": "US",
                            "exchange": row.get('exchange', ''),
                        },
                        "ipo_date": row.get('ipo_date'),
                        "status": row.get('status', 'Active'),
                        "updated_at": datetime.now(),
                        "data_source": "alphavantage",
                    }

                    operations.append(
                        UpdateOne(
                            {"code": symbol, "market": "US"},
                            {"$set": doc, "$setOnInsert": {"created_at": datetime.now()}},
                            upsert=True
                        )
                    )

                # 執行批量寫入 (分批處理避免超時)
                batch_size = 1000
                total_inserted = 0
                total_updated = 0
                
                for i in range(0, len(operations), batch_size):
                    batch = operations[i:i + batch_size]
                    result = await db[COLLECTION_NAME].bulk_write(batch, ordered=False)
                    total_inserted += result.upserted_count
                    total_updated += result.modified_count
                    logger.info(f"📝 批次 {i//batch_size + 1}: 新增 {result.upserted_count}, 更新 {result.modified_count}")

                stats.inserted = total_inserted
                stats.updated = total_updated
                logger.info(f"✅ 美股同步完成: 新增 {stats.inserted}, 更新 {stats.updated}")

                stats.status = "completed"
                stats.message = f"成功同步 {stats.total} 支美股"
                stats.finished_at = datetime.now().isoformat()

            except Exception as e:
                logger.error(f"❌ 美股同步失敗: {e}", exc_info=True)
                stats.status = "failed"
                stats.message = str(e)
                stats.errors = 1
                stats.finished_at = datetime.now().isoformat()

            finally:
                self._running = False
                await self._persist_status(db, stats.__dict__)

            return stats.__dict__

    async def sync_all(self, force: bool = False) -> Dict[str, Any]:
        """同步所有外國股票 (港股 + 美股)"""
        results = {
            "hk": None,
            "us": None,
        }
        
        # 先同步港股
        logger.info("🔄 開始同步港股...")
        results["hk"] = await self.sync_hk_stocks(force=force)
        
        # 再同步美股
        logger.info("🔄 開始同步美股...")
        results["us"] = await self.sync_us_stocks(force=force)
        
        return results

    async def sync_hk_quotes(self) -> Dict[str, Any]:
        """同步港股實時行情 (使用 AKShare)"""
        job_key = "quotes_hk"
        market = "HK"
        db = get_mongo_db()
        
        stats = {
            "job": job_key,
            "data_type": "quotes",
            "market": market,
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "data_source": "akshare",
            "total": 0,
            "updated": 0,
            "errors": 0,
        }
        
        await self._persist_status(db, stats)
        
        try:
            from app.services.data_sources.hk_akshare_adapter import HKAKShareAdapter
            
            adapter = HKAKShareAdapter()
            if not adapter.is_available():
                stats["status"] = "failed"
                stats["message"] = "AKShare 不可用"
                stats["finished_at"] = datetime.now().isoformat()
                await self._persist_status(db, stats)
                return stats

            logger.info("📈 開始同步港股實時行情...")
            
            # 獲取實時行情（get_stock_list 已包含價格信息）
            df = adapter.get_stock_list()
            if df is None or df.empty:
                stats["status"] = "failed"
                stats["message"] = "無法獲取港股行情"
                stats["finished_at"] = datetime.now().isoformat()
                await self._persist_status(db, stats)
                return stats

            stats["total"] = len(df)
            
            # 批量更新行情數據
            operations = []
            for _, row in df.iterrows():
                symbol = str(row.get('symbol', '')).strip()
                if not symbol:
                    continue

                update_doc = {
                    "price": row.get('price'),
                    "pct_change": row.get('pct_change'),
                    "volume": row.get('volume'),
                    "amount": row.get('amount'),
                    "quote_updated_at": datetime.now(),
                }

                operations.append(
                    UpdateOne(
                        {"code": symbol, "market": "HK"},
                        {"$set": update_doc}
                    )
                )

            if operations:
                result = await db[COLLECTION_NAME].bulk_write(operations, ordered=False)
                stats["updated"] = result.modified_count
                logger.info(f"✅ 港股行情同步完成: 更新 {stats['updated']} 支")

            stats["status"] = "completed"
            stats["message"] = f"成功更新 {stats['updated']} 支港股行情"
            stats["finished_at"] = datetime.now().isoformat()

        except Exception as e:
            logger.error(f"❌ 港股行情同步失敗: {e}", exc_info=True)
            stats["status"] = "failed"
            stats["message"] = str(e)
            stats["errors"] = 1
            stats["finished_at"] = datetime.now().isoformat()

        await self._persist_status(db, stats)
        return stats

    async def sync_us_quotes(self) -> Dict[str, Any]:
        """同步美股實時行情 (使用 Alpha Vantage - 注意 API 限制)"""
        job_key = "quotes_us"
        market = "US"
        db = get_mongo_db()
        
        stats = {
            "job": job_key,
            "data_type": "quotes",
            "market": market,
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "data_source": "alphavantage",
            "total": 0,
            "updated": 0,
            "errors": 0,
        }
        
        await self._persist_status(db, stats)
        
        try:
            from app.services.data_sources.us_alphavantage_adapter import USAlphaVantageAdapter
            
            adapter = USAlphaVantageAdapter()
            if not adapter.is_available():
                stats["status"] = "failed"
                stats["message"] = "Alpha Vantage API Key 未配置"
                stats["finished_at"] = datetime.now().isoformat()
                await self._persist_status(db, stats)
                return stats

            logger.info("📈 開始同步美股實時行情（僅自選股，因 API 限制）...")
            
            # 獲取自選股列表
            favorites = await db["favorites"].find({"market": "US"}).to_list(length=100)
            symbols = [f.get("symbol") or f.get("stock_code") for f in favorites if f.get("symbol") or f.get("stock_code")]
            
            if not symbols:
                # 如果沒有自選股，使用一些熱門股票
                symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "V", "JNJ"]
            
            stats["total"] = len(symbols)
            logger.info(f"📊 將同步 {len(symbols)} 支美股行情")
            
            # 逐個獲取行情（Alpha Vantage 免費版限制 5 次/分鐘）
            import time
            updated_count = 0
            
            for i, symbol in enumerate(symbols[:25]):  # 限制最多 25 支
                try:
                    quote = adapter.get_quote(symbol)
                    if quote:
                        await db[COLLECTION_NAME].update_one(
                            {"code": symbol, "market": "US"},
                            {"$set": {
                                "price": quote.get("price"),
                                "pct_change": quote.get("change_percent"),
                                "volume": quote.get("volume"),
                                "quote_updated_at": datetime.now(),
                            }}
                        )
                        updated_count += 1
                    
                    # API 限制：每分鐘 5 次
                    if (i + 1) % 5 == 0 and i < len(symbols) - 1:
                        logger.info(f"⏳ API 限制，等待 60 秒...")
                        time.sleep(60)
                        
                except Exception as e:
                    logger.warning(f"⚠️ 獲取 {symbol} 行情失敗: {e}")
                    stats["errors"] += 1

            stats["updated"] = updated_count
            stats["status"] = "completed"
            stats["message"] = f"成功更新 {updated_count} 支美股行情"
            stats["finished_at"] = datetime.now().isoformat()

        except Exception as e:
            logger.error(f"❌ 美股行情同步失敗: {e}", exc_info=True)
            stats["status"] = "failed"
            stats["message"] = str(e)
            stats["errors"] = 1
            stats["finished_at"] = datetime.now().isoformat()

        await self._persist_status(db, stats)
        return stats

    async def sync_hk_historical(self, days: int = 30) -> Dict[str, Any]:
        """同步港股歷史數據 (使用 AKShare)"""
        job_key = "historical_hk"
        market = "HK"
        db = get_mongo_db()
        
        stats = {
            "job": job_key,
            "data_type": "historical",
            "market": market,
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "data_source": "akshare",
            "total": 0,
            "inserted": 0,
            "errors": 0,
        }
        
        await self._persist_status(db, stats)
        
        try:
            from app.services.data_sources.hk_akshare_adapter import HKAKShareAdapter
            
            adapter = HKAKShareAdapter()
            if not adapter.is_available():
                stats["status"] = "failed"
                stats["message"] = "AKShare 不可用"
                stats["finished_at"] = datetime.now().isoformat()
                await self._persist_status(db, stats)
                return stats

            logger.info(f"📊 開始同步港股歷史數據（最近 {days} 天）...")
            
            # 獲取自選股或熱門股票
            favorites = await db["favorites"].find({"market": "HK"}).to_list(length=50)
            symbols = [f.get("symbol") or f.get("stock_code") for f in favorites if f.get("symbol") or f.get("stock_code")]
            
            if not symbols:
                # 使用一些熱門港股
                symbols = ["00700", "09988", "03690", "01810", "02318", "00941", "01299", "02020", "09618", "01024"]
            
            stats["total"] = len(symbols)
            total_inserted = 0
            
            for symbol in symbols:
                try:
                    df = adapter.get_daily_data(symbol, days=days)
                    if df is not None and not df.empty:
                        # 存入歷史數據集合
                        operations = []
                        for _, row in df.iterrows():
                            doc = {
                                "code": symbol,
                                "market": "HK",
                                "date": row.get("date"),
                                "open": row.get("open"),
                                "high": row.get("high"),
                                "low": row.get("low"),
                                "close": row.get("close"),
                                "volume": row.get("volume"),
                                "amount": row.get("amount"),
                                "updated_at": datetime.now(),
                            }
                            operations.append(
                                UpdateOne(
                                    {"code": symbol, "market": "HK", "date": row.get("date")},
                                    {"$set": doc},
                                    upsert=True
                                )
                            )
                        
                        if operations:
                            result = await db["stock_daily_data"].bulk_write(operations, ordered=False)
                            total_inserted += result.upserted_count
                            
                except Exception as e:
                    logger.warning(f"⚠️ 獲取 {symbol} 歷史數據失敗: {e}")
                    stats["errors"] += 1

            stats["inserted"] = total_inserted
            stats["status"] = "completed"
            stats["message"] = f"成功同步 {total_inserted} 條港股歷史數據"
            stats["finished_at"] = datetime.now().isoformat()

        except Exception as e:
            logger.error(f"❌ 港股歷史數據同步失敗: {e}", exc_info=True)
            stats["status"] = "failed"
            stats["message"] = str(e)
            stats["errors"] = 1
            stats["finished_at"] = datetime.now().isoformat()

        await self._persist_status(db, stats)
        return stats

    async def sync_us_historical(self, days: int = 30) -> Dict[str, Any]:
        """同步美股歷史數據 (使用 Alpha Vantage)"""
        job_key = "historical_us"
        market = "US"
        db = get_mongo_db()
        
        stats = {
            "job": job_key,
            "data_type": "historical",
            "market": market,
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "data_source": "alphavantage",
            "total": 0,
            "inserted": 0,
            "errors": 0,
        }
        
        await self._persist_status(db, stats)
        
        try:
            from app.services.data_sources.us_alphavantage_adapter import USAlphaVantageAdapter
            
            adapter = USAlphaVantageAdapter()
            if not adapter.is_available():
                stats["status"] = "failed"
                stats["message"] = "Alpha Vantage API Key 未配置"
                stats["finished_at"] = datetime.now().isoformat()
                await self._persist_status(db, stats)
                return stats

            logger.info(f"📊 開始同步美股歷史數據（最近 {days} 天）...")
            
            # 獲取自選股
            favorites = await db["favorites"].find({"market": "US"}).to_list(length=20)
            symbols = [f.get("symbol") or f.get("stock_code") for f in favorites if f.get("symbol") or f.get("stock_code")]
            
            if not symbols:
                symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
            
            stats["total"] = len(symbols)
            total_inserted = 0
            
            import time
            for i, symbol in enumerate(symbols[:10]):  # 限制最多 10 支
                try:
                    df = adapter.get_daily_data(symbol, days=days)
                    if df is not None and not df.empty:
                        operations = []
                        for _, row in df.iterrows():
                            doc = {
                                "code": symbol,
                                "market": "US",
                                "date": row.get("date"),
                                "open": row.get("open"),
                                "high": row.get("high"),
                                "low": row.get("low"),
                                "close": row.get("close"),
                                "volume": row.get("volume"),
                                "updated_at": datetime.now(),
                            }
                            operations.append(
                                UpdateOne(
                                    {"code": symbol, "market": "US", "date": row.get("date")},
                                    {"$set": doc},
                                    upsert=True
                                )
                            )
                        
                        if operations:
                            result = await db["stock_daily_data"].bulk_write(operations, ordered=False)
                            total_inserted += result.upserted_count
                    
                    # API 限制
                    if (i + 1) % 5 == 0 and i < len(symbols) - 1:
                        logger.info(f"⏳ API 限制，等待 60 秒...")
                        time.sleep(60)
                        
                except Exception as e:
                    logger.warning(f"⚠️ 獲取 {symbol} 歷史數據失敗: {e}")
                    stats["errors"] += 1

            stats["inserted"] = total_inserted
            stats["status"] = "completed"
            stats["message"] = f"成功同步 {total_inserted} 條美股歷史數據"
            stats["finished_at"] = datetime.now().isoformat()

        except Exception as e:
            logger.error(f"❌ 美股歷史數據同步失敗: {e}", exc_info=True)
            stats["status"] = "failed"
            stats["message"] = str(e)
            stats["errors"] = 1
            stats["finished_at"] = datetime.now().isoformat()

        await self._persist_status(db, stats)
        return stats

    async def check_hk_status(self) -> Dict[str, Any]:
        """檢查港股數據源狀態"""
        try:
            from app.services.data_sources.hk_akshare_adapter import HKAKShareAdapter
            adapter = HKAKShareAdapter()
            available = adapter.is_available()
            
            return {
                "source": "akshare_hk",
                "market": "HK",
                "available": available,
                "checked_at": datetime.now().isoformat(),
                "message": "港股數據源正常" if available else "港股數據源不可用"
            }
        except Exception as e:
            return {
                "source": "akshare_hk",
                "market": "HK",
                "available": False,
                "checked_at": datetime.now().isoformat(),
                "message": f"檢查失敗: {str(e)}"
            }

    async def check_us_status(self) -> Dict[str, Any]:
        """檢查美股數據源狀態"""
        try:
            from app.services.data_sources.us_alphavantage_adapter import USAlphaVantageAdapter
            adapter = USAlphaVantageAdapter()
            available = adapter.is_available()
            
            return {
                "source": "alphavantage_us",
                "market": "US",
                "available": available,
                "checked_at": datetime.now().isoformat(),
                "message": "美股數據源正常" if available else "美股數據源不可用（API Key 未配置）"
            }
        except Exception as e:
            return {
                "source": "alphavantage_us",
                "market": "US",
                "available": False,
                "checked_at": datetime.now().isoformat(),
                "message": f"檢查失敗: {str(e)}"
            }


# 全局實例
_foreign_sync_service: Optional[ForeignStockSyncService] = None


def get_foreign_stock_sync_service() -> ForeignStockSyncService:
    """獲取外國股票同步服務實例"""
    global _foreign_sync_service
    if _foreign_sync_service is None:
        _foreign_sync_service = ForeignStockSyncService()
    return _foreign_sync_service
