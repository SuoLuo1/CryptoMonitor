#!/usr/bin/env python3
"""
更新警报后续走势
每小时运行一次，检查警报后的价格变化
"""

import sqlite3
import requests
from datetime import datetime, timedelta

def get_price_at_time(symbol, target_time):
    """通过 K 线获取指定时间的价格"""
    try:
        start_time = int(target_time.timestamp() * 1000)
        end_time = int((target_time + timedelta(hours=1)).timestamp() * 1000)
        
        # 使用 1h K 线 (兼容性最好)
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1h',
            'startTime': start_time,
            'endTime': end_time,
            'limit': 3
        }
        
        resp = requests.get(url, params=params, timeout=5)
        
        if resp.status_code == 200:
            data = resp.json()
            if data:
                return float(data[0][4])  # 收盘价
    
    except Exception as e:
        pass
    
    return None

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
    
    print(f"📊 待分析警报：{len(alerts)} 条\n")
    
    updated = 0
    kline_success = 0
    fallback_success = 0
    failed = 0
    
    for alert_id, symbol, alert_time_str, price_at_alert in alerts:
        try:
            alert_time = datetime.strptime(alert_time_str, '%Y-%m-%d %H:%M:%S')
            time_1h_after = alert_time + timedelta(hours=1)
            
            # 方法 1: K 线历史数据
            price_1h = get_price_at_time(symbol, time_1h_after)
            
            # 方法 2: Fallback 到当前价格
            use_fallback = False
            if not price_1h:
                price_1h = get_current_price(symbol)
                use_fallback = True
            
            if price_1h and price_at_alert and price_at_alert > 0:
                change_pct = (price_1h - price_at_alert) / price_at_alert * 100
                is_success = 1 if change_pct > 10 else 0
                
                cursor.execute('''
                    UPDATE alerts
                    SET price_1h_after = ?, change_1h_pct = ?, is_success = ?, analyzed = 1
                    WHERE id = ?
                ''', (price_1h, change_pct, is_success, alert_id))
                
                updated += 1
                if use_fallback:
                    fallback_success += 1
                else:
                    kline_success += 1
                
                status = "✅" if is_success else "❌"
                fb = " (FB)" if use_fallback else ""
                print(f"  {symbol}: {change_pct:+.1f}% {status}{fb}")
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
