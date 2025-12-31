"""
港股和美股定時同步任務
- 港股: AKShare
- 美股: Alpha Vantage
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def run_hk_stock_sync():
    """運行港股數據同步任務"""
    try:
        logger.info("🇭🇰 開始港股數據同步...")
        
        from app.services.foreign_stock_sync_service import get_foreign_stock_sync_service
        
        service = get_foreign_stock_sync_service()
        result = await service.sync_hk_stocks(force=False)
        
        if result.get("status") == "completed":
            logger.info(
                f"✅ 港股同步完成: "
                f"總數 {result.get('total', 0)}, "
                f"新增 {result.get('inserted', 0)}, "
                f"更新 {result.get('updated', 0)}"
            )
        else:
            logger.warning(f"⚠️ 港股同步未完成: {result.get('message', 'Unknown error')}")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ 港股同步失敗: {e}", exc_info=True)
        return {"status": "failed", "message": str(e)}


async def run_us_stock_sync():
    """運行美股數據同步任務"""
    try:
        logger.info("🇺🇸 開始美股數據同步...")
        
        from app.services.foreign_stock_sync_service import get_foreign_stock_sync_service
        
        service = get_foreign_stock_sync_service()
        result = await service.sync_us_stocks(force=False)
        
        if result.get("status") == "completed":
            logger.info(
                f"✅ 美股同步完成: "
                f"總數 {result.get('total', 0)}, "
                f"新增 {result.get('inserted', 0)}, "
                f"更新 {result.get('updated', 0)}"
            )
        else:
            logger.warning(f"⚠️ 美股同步未完成: {result.get('message', 'Unknown error')}")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ 美股同步失敗: {e}", exc_info=True)
        return {"status": "failed", "message": str(e)}


async def run_foreign_stock_sync():
    """運行所有外國股票同步任務 (港股 + 美股)"""
    try:
        logger.info("🌏 開始外國股票數據同步...")
        
        from app.services.foreign_stock_sync_service import get_foreign_stock_sync_service
        
        service = get_foreign_stock_sync_service()
        result = await service.sync_all(force=False)
        
        hk_result = result.get("hk", {})
        us_result = result.get("us", {})
        
        logger.info(
            f"✅ 外國股票同步完成: "
            f"港股 {hk_result.get('total', 0)} 支 ({hk_result.get('status', 'N/A')}), "
            f"美股 {us_result.get('total', 0)} 支 ({us_result.get('status', 'N/A')})"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"❌ 外國股票同步失敗: {e}", exc_info=True)
        return {"status": "failed", "message": str(e)}


async def run_hk_quotes_sync():
    """運行港股實時行情同步任務"""
    try:
        logger.info("📈 開始港股實時行情同步...")
        
        from app.services.foreign_stock_sync_service import get_foreign_stock_sync_service
        
        service = get_foreign_stock_sync_service()
        result = await service.sync_hk_quotes()
        
        if result.get("status") == "completed":
            logger.info(f"✅ 港股行情同步完成: 更新 {result.get('updated', 0)} 支")
        else:
            logger.warning(f"⚠️ 港股行情同步未完成: {result.get('message', 'Unknown error')}")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ 港股行情同步失敗: {e}", exc_info=True)
        return {"status": "failed", "message": str(e)}


async def run_us_quotes_sync():
    """運行美股實時行情同步任務"""
    try:
        logger.info("📈 開始美股實時行情同步...")
        
        from app.services.foreign_stock_sync_service import get_foreign_stock_sync_service
        
        service = get_foreign_stock_sync_service()
        result = await service.sync_us_quotes()
        
        if result.get("status") == "completed":
            logger.info(f"✅ 美股行情同步完成: 更新 {result.get('updated', 0)} 支")
        else:
            logger.warning(f"⚠️ 美股行情同步未完成: {result.get('message', 'Unknown error')}")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ 美股行情同步失敗: {e}", exc_info=True)
        return {"status": "failed", "message": str(e)}


async def run_hk_historical_sync():
    """運行港股歷史數據同步任務"""
    try:
        logger.info("📊 開始港股歷史數據同步...")
        
        from app.services.foreign_stock_sync_service import get_foreign_stock_sync_service
        
        service = get_foreign_stock_sync_service()
        result = await service.sync_hk_historical(days=30)
        
        if result.get("status") == "completed":
            logger.info(f"✅ 港股歷史數據同步完成: 新增 {result.get('inserted', 0)} 條")
        else:
            logger.warning(f"⚠️ 港股歷史數據同步未完成: {result.get('message', 'Unknown error')}")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ 港股歷史數據同步失敗: {e}", exc_info=True)
        return {"status": "failed", "message": str(e)}


async def run_us_historical_sync():
    """運行美股歷史數據同步任務"""
    try:
        logger.info("📊 開始美股歷史數據同步...")
        
        from app.services.foreign_stock_sync_service import get_foreign_stock_sync_service
        
        service = get_foreign_stock_sync_service()
        result = await service.sync_us_historical(days=30)
        
        if result.get("status") == "completed":
            logger.info(f"✅ 美股歷史數據同步完成: 新增 {result.get('inserted', 0)} 條")
        else:
            logger.warning(f"⚠️ 美股歷史數據同步未完成: {result.get('message', 'Unknown error')}")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ 美股歷史數據同步失敗: {e}", exc_info=True)
        return {"status": "failed", "message": str(e)}


async def run_hk_status_check():
    """運行港股數據源狀態檢查"""
    try:
        logger.info("🔍 開始港股數據源狀態檢查...")
        
        from app.services.foreign_stock_sync_service import get_foreign_stock_sync_service
        
        service = get_foreign_stock_sync_service()
        result = await service.check_hk_status()
        
        if result.get("available"):
            logger.info(f"✅ 港股數據源狀態: 正常")
        else:
            logger.warning(f"⚠️ 港股數據源狀態: {result.get('message', '不可用')}")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ 港股狀態檢查失敗: {e}", exc_info=True)
        return {"available": False, "message": str(e)}


async def run_us_status_check():
    """運行美股數據源狀態檢查"""
    try:
        logger.info("🔍 開始美股數據源狀態檢查...")
        
        from app.services.foreign_stock_sync_service import get_foreign_stock_sync_service
        
        service = get_foreign_stock_sync_service()
        result = await service.check_us_status()
        
        if result.get("available"):
            logger.info(f"✅ 美股數據源狀態: 正常")
        else:
            logger.warning(f"⚠️ 美股數據源狀態: {result.get('message', '不可用')}")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ 美股狀態檢查失敗: {e}", exc_info=True)
        return {"available": False, "message": str(e)}
