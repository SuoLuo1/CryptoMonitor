"""
币安 API 权重管理器

功能:
1. 追踪每个 API 的权重消耗
2. 实时计算当前分钟已用权重
3. 接近 2400 时主动暂停请求
4. 跨项目共享 (Redis)

权重限制:
- 2400 weight / MINUTE
- 安全阈值：2000 (83%)
"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
import json

from shared.logging import get_logger
from shared.config import config
from shared.cache import cache

logger = get_logger("binance_rate_limiter")


@dataclass
class WeightRecord:
    """权重记录"""
    weight: int  # 消耗的权重
    timestamp: float  # 请求时间戳
    
    def to_dict(self) -> Dict:
        return {
            "weight": self.weight,
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "WeightRecord":
        return cls(
            weight=data["weight"],
            timestamp=data["timestamp"]
        )


class BinanceRateLimiter:
    """
    币安 API 限流管理器
    
    权重限制:
    - 2400 weight / MINUTE (官方限制)
    - 2000 weight / MINUTE (安全阈值，83%)
    """
    
    # 官方限制
    MAX_WEIGHT_PER_MINUTE = 2400
    # 安全阈值 (83%)
    SAFE_THRESHOLD = 2000
    # 警告阈值 (70%)
    WARNING_THRESHOLD = 1680
    
    # 缓存键
    CACHE_KEY_WEIGHTS = "binance:api:weights"
    CACHE_KEY_LAST_CHECK = "binance:api:last_check"
    CACHE_KEY_BLOCKED = "binance:api:blocked"
    
    def __init__(self):
        self._current_minute_weights: list[WeightRecord] = []
        self._last_cleanup: float = 0
        self._cleanup_interval = 10.0  # 10 秒清理一次
        
        # 从配置读取阈值
        binance_config = config.get("rate_limit.binance", {})
        self.MAX_WEIGHT_PER_MINUTE = binance_config.get("weight_per_minute", 2400)
        self.safe_threshold = binance_config.get("safe_threshold", 2000)
        self.warning_threshold = binance_config.get("warning_threshold", 1680)
        self.timeout = binance_config.get("timeout", 60)
        
        # API 权重定义
        self.api_weights = binance_config.get("weights", {
            "exchangeInfo": 40,
            "ticker/24hr": 2,
            "ticker/book": 2,
            "depth": 5,
            "fundingRate": 2,
            "openInterest": 2,
            "klines": 2,
        })
        
        logger.info(
            f"币安限流器初始化：安全阈值={self.safe_threshold}, "
            f"警告阈值={self.warning_threshold}, 最大权重={self.MAX_WEIGHT_PER_MINUTE}"
        )
    
    def _get_current_minute(self) -> float:
        """获取当前分钟的起始时间戳"""
        now = time.time()
        return now - (now % 60)
    
    def _cleanup_old_records(self):
        """清理过期的权重记录"""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return
        
        current_minute = self._get_current_minute()
        # 只保留当前分钟的记录
        self._current_minute_weights = [
            r for r in self._current_minute_weights
            if r.timestamp >= current_minute
        ]
        self._last_cleanup = now
    
    def _get_total_weight(self) -> int:
        """获取当前分钟已用权重"""
        self._cleanup_old_records()
        return sum(r.weight for r in self._current_minute_weights)
    
    def _add_weight(self, weight: int):
        """添加权重记录"""
        record = WeightRecord(weight=weight, timestamp=time.time())
        self._current_minute_weights.append(record)
        
        # 同步到 Redis (跨项目共享)
        try:
            weights_data = [r.to_dict() for r in self._current_minute_weights]
            cache.set(self.CACHE_KEY_WEIGHTS, json.dumps(weights_data), ttl=120)
        except Exception as e:
            logger.debug(f"同步权重到 Redis 失败：{e}")
    
    def _load_from_redis(self):
        """从 Redis 加载权重数据"""
        try:
            data = cache.get(self.CACHE_KEY_WEIGHTS)
            if data:
                weights_list = json.loads(data)
                self._current_minute_weights = [
                    WeightRecord.from_dict(r) for r in weights_list
                ]
                # 清理过期记录
                self._cleanup_old_records()
        except Exception as e:
            logger.debug(f"从 Redis 加载权重失败：{e}")
    
    def _check_blocked(self) -> bool:
        """检查是否被币安 ban"""
        try:
            blocked = cache.get(self.CACHE_KEY_BLOCKED)
            if blocked:
                unblock_time = float(blocked)
                if time.time() < unblock_time:
                    remaining = int(unblock_time - time.time())
                    logger.warning(f"IP 被币安 ban，还需等待 {remaining} 秒")
                    return True
                else:
                    cache.delete(self.CACHE_KEY_BLOCKED)
                    logger.info("IP 已从币安 ban 中恢复")
        except Exception as e:
            logger.debug(f"检查 ban 状态失败：{e}")
        return False
    
    def _set_blocked(self, unblock_time: float):
        """设置 ban 状态"""
        try:
            cache.set(self.CACHE_KEY_BLOCKED, str(unblock_time), ttl=3600)
            logger.warning(f"IP 被币安 ban，解封时间：{datetime.fromtimestamp(unblock_time)}")
        except Exception as e:
            logger.debug(f"设置 ban 状态失败：{e}")
    
    async def can_request(self, weight: int = 1) -> bool:
        """
        检查是否可以发起请求
        
        Args:
            weight: 预计消耗的权重
        
        Returns:
            True 可以请求，False 需要等待
        """
        # 检查是否被 ban
        if self._check_blocked():
            return False
        
        # 从 Redis 加载最新数据
        self._load_from_redis()
        
        # 获取当前权重
        total = self._get_total_weight()
        
        # 超过安全阈值，拒绝请求
        if total + weight > self.safe_threshold:
            logger.warning(f"权重接近限制：{total}/{self.safe_threshold}，拒绝请求")
            return False
        
        # 超过警告阈值，记录警告
        if total + weight > self.warning_threshold:
            logger.warning(f"权重警告：{total + weight}/{self.safe_threshold}")
        
        return True
    
    async def wait_if_needed(self, weight: int = 1, timeout: float = 60.0):
        """
        如果需要等待，则阻塞直到可以请求
        
        Args:
            weight: 预计消耗的权重
            timeout: 最大等待时间 (秒)
        
        Returns:
            True 可以请求，False 超时
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if await self.can_request(weight):
                return True
            
            # 等待 1 秒后重试
            await asyncio.sleep(1)
        
        logger.error(f"等待权重恢复超时 ({timeout}秒)")
        return False
    
    def record_request(self, weight: int, endpoint: str = "unknown"):
        """
        记录一次请求的权重消耗
        
        Args:
            weight: 实际消耗的权重
            endpoint: API 端点
        """
        self._add_weight(weight)
        total = self._get_total_weight()
        
        if total > self.warning_threshold:
            logger.info(f"权重消耗：+{weight} ({endpoint}) => 总计 {total}/{self.safe_threshold}")
    
    async def request_with_limit(
        self,
        weight: int,
        endpoint: str = "unknown",
        timeout: float = 60.0
    ) -> bool:
        """
        带限流检查的请求包装器
        
        用法:
            if await limiter.request_with_limit(2, "/fapi/v1/ticker/24hr"):
                # 发起请求
                limiter.record_request(2, "/fapi/v1/ticker/24hr")
        
        Args:
            weight: 预计消耗的权重
            endpoint: API 端点
            timeout: 最大等待时间
        
        Returns:
            True 可以发起请求，False 需要跳过
        """
        if await self.wait_if_needed(weight, timeout):
            return True
        return False
    
    def get_status(self) -> Dict[str, Any]:
        """获取当前状态"""
        self._load_from_redis()
        total = self._get_total_weight()
        
        return {
            "current_weight": total,
            "safe_threshold": self.safe_threshold,
            "warning_threshold": self.warning_threshold,
            "max_weight": self.MAX_WEIGHT_PER_MINUTE,
            "usage_percent": (total / self.MAX_WEIGHT_PER_MINUTE) * 100,
            "is_blocked": self._check_blocked(),
            "record_count": len(self._current_minute_weights),
        }


# 全局单例
limiter = BinanceRateLimiter()


def get_limiter() -> BinanceRateLimiter:
    """获取全局限流器实例"""
    return limiter
