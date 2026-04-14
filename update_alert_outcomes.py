#!/usr/bin/env python3
"""
更新警报后续走势
每小时运行一次，检查警报后的价格变化

修复:
1. 根据 signal_type 区分多空方向判定成功
2. 使用 1m K 线获取精确时间点价格
3. 根据信号类型使用不同验证窗口
"""

import sqlite3
import requests
from datetime import datetime, timedelta

# 信号类型分类
BULLISH_SIGNALS = ["🟢 做多", "🟢 巨鲸进场", "🟢 增仓", "🟢 暴涨", "支撑薄弱", "正溢价", "🟢 金叉"]
BEARISH_SIGNALS = ["🔴 做空", "🔴 巨鲸出货", "🔴 减仓", "🔴 暴跌", "阻力薄弱", "负溢价", "🔴 死叉"]

# 不同信号类型的验证窗口 (分钟)
VERIFICATION_WINDOWS = {
    "暴涨": 15,      # 暴涨信号 15 分钟内验证
    "暴跌": 15,      # 暴跌信号 15 分钟内验证
    "巨鲸": 30,      # 巨鲸信号 30 分钟内验证
    "增仓": 30,      # OI 增仓 30 分钟内验证
    "减仓": 30,      # OI 减仓 30 分钟内验证
    "爆仓": 30,      # 爆仓信号 30 分钟内验证
    "溢价": 60,      # 溢价信号 1 小时内验证
    "金叉": 60,      # 技术信号 1 小时内验证
    "死叉": 60,      # 技术信号 1 小时内验证
    "default": 60,   # 默认 1 小时
}

# 不同信号类型的成功阈值 (%)
SUCCESS_THRESHOLDS = {
    "bullish": 10,   # 做多信号涨超 10% 算成功
    "bearish": -10,  # 做空信号跌超 10% 算成功
}


def get_price_at_time(symbol, target_time, window_minutes=60):
    """
    通过 1m K 线获取指定时间的精确价格
    
    Args:
        symbol: 交易对
        target_time: 目标时间
        window_minutes: 验证窗口 (分钟)
    """
    try:
        start_time = int(target_time.timestamp() * 1000)
        # 获取目标时间后 window_minutes 分钟的 K 线
        end_time = int((target_time + timedelta(minutes=window_minutes)).timestamp() * 1000)
        
        # 使用 1m K 线获取精确价格
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1m',
            'startTime': start_time,
            'endTime': end_time,
            'limit': min(window_minutes + 1, 1000)  # 最多 1000 根
        }
        
        resp = requests.get(url, params=params, timeout=5)
        
        if resp.status_code == 200:
            data = resp.json()
            if data:
                # 返回验证窗口内的最高/最低价 (取对信号方最有利的价格)
                prices = [float(k[4]) for k in data]  # 收盘价
                return max(prices)  # 做多信号看最高价，做空信号看最低价 (外面会处理)
    except Exception as e:
        pass
    
    return None


def get_price_range(symbol, alert_time, window_minutes=60):
    """
    获取验证窗口内的最高价和最低价
    
    Returns:
        (high_price, low_price) 或 (None, None)
    """
    try:
        start_time = int(alert_time.timestamp() * 1000)
        end_time = int((alert_time + timedelta(minutes=window_minutes)).timestamp() * 1000)
        
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1m',
            'startTime': start_time,
            'endTime': end_time,
            'limit': min(window_minutes + 1, 1000)
        }
        
        resp = requests.get(url, params=params, timeout=5)
        
        if resp.status_code == 200:
            data = resp.json()
            if data:
                highs = [float(k[2]) for k in data]  # 最高价
                lows = [float(k[3]) for k in data]   # 最低价
                return max(highs), min(lows)
    except Exception as e:
        pass
    
    return None, None

def get_current_price(symbol):
    """获取当前价格"""
    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/price"
        params = {'symbol': symbol}
        resp = requests.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            return float(resp.json()['price'])
    except:
        pass
    return None

def update_outcomes():
    conn = sqlite3.connect('/root/CryptoMonitor/alerts.db')
    cursor = conn.cursor()
    
    # 查询时包含 signal_type 用于判断多空方向
    cursor.execute('''
        SELECT id, symbol, signal_type, timestamp, price_at_alert
        FROM alerts
        WHERE analyzed = 0
        AND timestamp < datetime('now', 'localtime', '-1 hour')
        ORDER BY timestamp
    ''')
    
    alerts = cursor.fetchall()
    
    if not alerts:
        print("⏳ 暂无待分析的警报")
        conn.close()
        return
    
    print(f"📊 待分析警报：{len(alerts)} 条\n")
    
    updated = 0
    kline_success = 0
    fallback_success = 0
    failed = 0
    
    for alert_id, symbol, signal_type, alert_time_str, price_at_alert in alerts:
        try:
            alert_time = datetime.strptime(alert_time_str, '%Y-%m-%d %H:%M:%S')
            
            # 1. 根据信号类型确定验证窗口
            window_minutes = VERIFICATION_WINDOWS.get("default")
            for key, val in VERIFICATION_WINDOWS.items():
                if key in signal_type:
                    window_minutes = val
                    break
            
            # 2. 判断信号方向 (做多/做空)
            is_bullish = any(s in signal_type for s in BULLISH_SIGNALS)
            is_bearish = any(s in signal_type for s in BEARISH_SIGNALS)
            
            # 3. 获取验证窗口内的价格区间
            high_price, low_price = get_price_range(symbol, alert_time, window_minutes)
            
            # 4. Fallback 到当前价格 (如果 K 线获取失败)
            use_fallback = False
            if high_price is None or low_price is None:
                current_price = get_current_price(symbol)
                if current_price:
                    high_price = low_price = current_price
                    use_fallback = True
            
            if high_price and low_price and price_at_alert and price_at_alert > 0:
                # 5. 根据信号方向判定成功
                if is_bullish:
                    # 做多信号：看最高价涨超阈值
                    change_pct = (high_price - price_at_alert) / price_at_alert * 100
                    threshold = SUCCESS_THRESHOLDS["bullish"]
                    is_success = 1 if change_pct >= threshold else 0
                elif is_bearish:
                    # 做空信号：看最低价跌超阈值
                    change_pct = (low_price - price_at_alert) / price_at_alert * 100
                    threshold = SUCCESS_THRESHOLDS["bearish"]
                    is_success = 1 if change_pct <= threshold else 0
                else:
                    # 未知方向：默认按做多判断
                    change_pct = (high_price - price_at_alert) / price_at_alert * 100
                    is_success = 1 if change_pct >= 10 else 0
                
                cursor.execute('''
                    UPDATE alerts
                    SET price_1h_after = ?, change_1h_pct = ?, is_success = ?, analyzed = 1
                    WHERE id = ?
                ''', (high_price, change_pct, is_success, alert_id))
                
                updated += 1
                if use_fallback:
                    fallback_success += 1
                else:
                    kline_success += 1
                
                # 显示结果
                direction = "多" if is_bullish else "空" if is_bearish else "?"
                status = "✅" if is_success else "❌"
                fb = " (FB)" if use_fallback else ""
                print(f"  {symbol} [{direction}]: {change_pct:+.1f}% {status}{fb} (窗口:{window_minutes}m)")
            else:
                failed += 1
                print(f"  ❌ {symbol}: 价格无效 (警报={price_at_alert})")
        
        except Exception as e:
            failed += 1
            print(f"  ❌ {symbol}: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ 已更新 {updated}/{len(alerts)} 条")
    print(f"   K 线成功：{kline_success} 条")
    print(f"   Fallback: {fallback_success} 条")
    print(f"   失败：{failed} 条")

if __name__ == '__main__':
    update_outcomes()
