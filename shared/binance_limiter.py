"""
币安 API 限流器 - 从响应头读取权重

无需 Redis！每个项目从 API 响应头读取的就是全局权重！

币安 API 响应头:
- X-MBX-USED-WEIGHT-1M: 当前分钟已用权重（全局）
- X-MBX-USED-WEIGHT-1S: 当前秒已用权重
- Retry-After: 被封禁时的解封时间（秒）
"""
import time
import json
from typing import Optional, Dict, Any
from datetime import datetime

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


class BinanceLimiter:
    """币安限流器 - 从响应头读取权重（无需 Redis）"""
    
    # 配置
    MAX_WEIGHT = 2400
    SAFE_THRESHOLD = 2200      # 92% - 提高阈值允许发图
    WARNING_THRESHOLD = 1680   # 70%
    
    # 内存存储
    _current_weight = 0
    _last_update = 0
    _blocked_until = 0
    
    @classmethod
    def update_weight(cls, weight: int):
        """更新权重（从 API 响应头）"""
        cls._current_weight = weight
        cls._last_update = time.time()
        
        if weight > cls.WARNING_THRESHOLD:
            print(f"[{datetime.now()}] ⚠️ 权重警告：{weight}/{cls.SAFE_THRESHOLD}")
        
        if weight > cls.SAFE_THRESHOLD:
            print(f"[{datetime.now()}] 🔴 权重超限：{weight}/{cls.SAFE_THRESHOLD}，请降低请求频率")
    
    @classmethod
    def set_blocked(cls, seconds: int):
        """设置被封禁"""
        cls._blocked_until = time.time() + seconds
        unblock_time = datetime.fromtimestamp(cls._blocked_until)
        print(f"[{datetime.now()}] ❌ IP 被币安 ban，解封时间：{unblock_time} ({seconds}秒后)")
    
    @classmethod
    def is_blocked(cls) -> bool:
        """检查是否被封禁"""
        if time.time() < cls._blocked_until:
            remaining = int(cls._blocked_until - time.time())
            print(f"[{datetime.now()}] ❌ IP 仍被封禁，还需等待 {remaining} 秒")
            return True
        return False
    
    @classmethod
    def can_request(cls, weight: int = 1) -> bool:
        """检查是否可以请求"""
        if cls.is_blocked():
            return False
        
        # 🔄 币安权重每分钟重置，检查是否需要重置
        if time.time() - cls._last_update > 60:
            print(f"[{datetime.now()}] 🔄 权重重置：{cls._current_weight} -> 0 (已过 60 秒)")
            cls._current_weight = 0
            cls._last_update = time.time()
        
        if cls._current_weight + weight > cls.SAFE_THRESHOLD:
            print(f"[{datetime.now()}] ⚠️ 权重接近限制：{cls._current_weight}/{cls.SAFE_THRESHOLD}，跳过请求")
            return False
        
        return True
    
    @classmethod
    def get_status(cls) -> Dict[str, Any]:
        """获取状态"""
        return {
            "current_weight": cls._current_weight,
            "safe_threshold": cls.SAFE_THRESHOLD,
            "usage_percent": (cls._current_weight / cls.MAX_WEIGHT) * 100,
            "is_blocked": cls.is_blocked(),
        }


# 全局实例
limiter = BinanceLimiter()


# ============== 同步请求包装器 (requests) ==============

def limited_get(url: str, weight: int = 2, endpoint: str = "", timeout: int = 10, **kwargs):
    """
    带限流的 requests.get
    
    从响应头读取 X-MBX-USED-WEIGHT-1M
    
    用法:
        response = limited_get("https://fapi.binance.com/fapi/v1/ticker/24hr", weight=2)
        if response:
            data = response.json()
    """
    if not HAS_REQUESTS:
        raise ImportError("需要安装 requests: pip install requests")
    
    # 检查限流
    if not limiter.can_request(weight):
        return None
    
    # 发起请求
    try:
        response = requests.get(url, timeout=timeout, **kwargs)
        
        # 🎯 从响应头读取权重（全局权重，所有项目共享）
        used_weight = response.headers.get('X-MBX-USED-WEIGHT-1M')
        if used_weight:
            weight_value = int(used_weight)
            limiter.update_weight(weight_value)
        
        # 检查 418 (IP 被封禁)
        if response.status_code == 418:
            retry_after = response.headers.get('Retry-After', '300')
            limiter.set_blocked(int(retry_after))
            return None
        
        return response
        
    except Exception as e:
        print(f"[{datetime.now()}] ❌ 请求失败：{e}")
        return None


# ============== 异步请求包装器 (aiohttp) ==============

try:
    import aiohttp
    
    class LimitedSession:
        """带限流的 aiohttp ClientSession 包装器"""
        
        def __init__(self, session, weight: int, endpoint: str = ""):
            self.session = session
            self.weight = weight
            self.endpoint = endpoint
        
        async def get(self, url, **kwargs):
            if not limiter.can_request(self.weight):
                return None
            
            async with self.session.get(url, **kwargs) as response:
                # 🎯 从响应头读取权重
                used_weight = response.headers.get('X-MBX-USED-WEIGHT-1M')
                if used_weight:
                    weight_value = int(used_weight)
                    limiter.update_weight(weight_value)
                
                if response.status == 418:
                    retry_after = response.headers.get('Retry-After', '300')
                    limiter.set_blocked(int(retry_after))
                    return None
                
                return response
    
    async def limited_aiohttp_session(weight: int = 2, endpoint: str = ""):
        """创建带限流的 aiohttp session"""
        if not limiter.can_request(weight):
            return None
        
        session = aiohttp.ClientSession()
        return LimitedSession(session, weight, endpoint)

except ImportError:
    pass
