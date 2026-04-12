#!/usr/bin/env python3
"""
更新警报后续走势 (改进版)
每小时运行一次，通过 K 线历史数据获取准确的 1h 后价格
"""

import sqlite3
import requests
from datetime import datetime, timedelta

def get_price_at_time(symbol, target_time):
    """
    通过 K 线获取指定时间的价格
    
    Args:
        symbol: 币种 (如 JCTUSDT)
        target_time: 目标时间 (datetime 对象)
    
    Returns:
        该时间的收盘价，或 None
    """
    try:
        # 转换为毫秒时间戳
        start_time = int(target_time.timestamp() * 1000)
        end_time = int((target_time + timedelta(minutes=5)).timestamp() * 1000)
        
        # 获取 1m K 线 (最精确)
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1m',
            'startTime': start_time,
            'endTime': end_time,
            'limit': 5
        }
        
        resp = requests.get(url, params=params, timeout=5)
        
        if resp.status_code == 200:
            data = resp.json()
            if data:
                # 返回第一根 K 线的收盘价
                close_price = float(data[0][4])
                return close_price
    
    except Exception as e:
        print(f"❌ 获取 K 线失败 {symbol} @{target_time}: {e}")
    
    return None

def get_current_price(symbol):
    """获取当前价格 (fallback)"""
    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/price"
        params = {'symbol': symbol}
        resp = requests.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            return float(data['price'])
    except Exception as e:
        print(f"❌ 获取当前价失败 {symbol}: {e}")
    return None

def update_outcomes():
    conn = sqlite3.connect('/root/CryptoMonitor/alerts.db')
    cursor = conn.cursor()
    
    # 查询未分析的警报 (1 小时前的)
    cursor.execute('''
        SELECT id, symbol, timestamp, price_at_alert
        FROM alerts
        WHERE analyzed = 0
        AND timestamp < datetime('now', '-1 hour')
        ORDER BY timestamp
    ''')
    
    alerts = cursor.fetchall()
    
    if not alerts:
        print("⏳ 暂无待分析的警报")
        conn.close()
        return
    
    print(f"📊 待分析警报：{len(alerts)} 条")
    
    updated_count = 0
    failed_count = 0
    
    for alert_id, symbol, alert_time_str, price_at_alert in alerts:
        try:
            # 解析警报时间
            alert_time = datetime.strptime(alert_time_str, '%Y-%m-%d %H:%M:%S')
            
            # 计算 1h 后的时间
            time_1h_after = alert_time + timedelta(hours=1)
            
            # ✅ 通过 K 线获取 1h 后的价格
            price_1h = get_price_at_time(symbol, time_1h_after)
            
            if price_1h and price_at_alert:
                change_pct = (price_1h - price_at_alert) / price_at_alert * 100
                
                # 判断是否成功 (1h 内涨幅>10%)
                is_success = 1 if change_pct > 10 else 0
                
                cursor.execute('''
                    UPDATE alerts
                    SET price_1h_after = ?,
                        change_1h_pct = ?,
                        is_success = ?,
                        analyzed = 1
                    WHERE id = ?
                ''', (price_1h, change_pct, is_success, alert_id))
                
                updated_count += 1
                status = "✅ 成功" if is_success else "❌ 失败"
                print(f"  {symbol}: {change_pct:+.1f}% ({status})")
            else:
                # Fallback: 使用当前价格
                current_price = get_current_price(symbol)
                if current_price and price_at_alert:
                    change_pct = (current_price - price_at_alert) / price_at_alert * 100
                    is_success = 1 if change_pct > 10 else 0
                    
                    cursor.execute('''
                        UPDATE alerts
                        SET price_1h_after = ?,
                            change_1h_pct = ?,
                            is_success = ?,
                            analyzed = 1
                        WHERE id = ?
                    ''', (current_price, change_pct, is_success, alert_id))
                    
                    updated_count += 1
                    print(f"  {symbol}: {change_pct:+.1f}% (使用当前价)")
                else:
                    failed_count += 1
                    print(f"  ❌ {symbol}: 无法获取价格")
        
        except Exception as e:
            failed_count += 1
            print(f"❌ 更新失败 {symbol}: {e}")
    
    conn.commit()
    conn.close()
    print(f"\n✅ 已更新 {updated_count}/{len(alerts)} 条警报结果")
    if failed_count > 0:
        print(f"⚠️ 失败 {failed_count} 条")

if __name__ == '__main__':
    update_outcomes()
