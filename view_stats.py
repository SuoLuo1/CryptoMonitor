#!/usr/bin/env python3
"""
快速查看警报统计
"""

import sqlite3

def quick_stats():
    conn = sqlite3.connect('/root/CryptoMonitor/alerts.db')
    cursor = conn.cursor()
    
    # 总览
    cursor.execute('SELECT COUNT(*) FROM alerts')
    total = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM alerts WHERE analyzed = 1')
    analyzed = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM alerts WHERE is_success = 1')
    success = cursor.fetchone()[0]
    
    print("\n" + "="*50)
    print("📊 CryptoMonitor 警报统计")
    print("="*50)
    print(f"📁 总警报数：{total}")
    print(f"✅ 已分析：{analyzed}")
    print(f"🎯 成功数：{success} ({success/analyzed*100:.1f}%)" if analyzed > 0 else "⏳ 等待分析...")
    print("="*50)
    
    # 今日警报
    cursor.execute('''
        SELECT COUNT(*) FROM alerts 
        WHERE date(timestamp) = date('now')
    ''')
    today = cursor.fetchone()[0]
    print(f"📅 今日警报：{today}")
    
    # 最近成功
    cursor.execute('''
        SELECT symbol, signal_type, change_1h_pct, timestamp
        FROM alerts
        WHERE is_success = 1
        ORDER BY timestamp DESC
        LIMIT 5
    ''')
    recent = cursor.fetchall()
    
    if recent:
        print("\n🔥 最近成功:")
        for row in recent:
            print(f"  {row[0]:<15} {row[1]:<10} {row[2]:+.1f}% ({row[3]})")
    
    conn.close()
    print("\n" + "="*50)

if __name__ == '__main__':
    quick_stats()
