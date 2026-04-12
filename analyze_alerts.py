#!/usr/bin/env python3
"""
分析警报历史
找出成功的警报有什么特征
"""

import sqlite3
from datetime import datetime

def analyze_success_rate():
    conn = sqlite3.connect('/root/CryptoMonitor/alerts.db')
    cursor = conn.cursor()
    
    # 查询所有已分析的警报
    cursor.execute('''
        SELECT 
            symbol,
            signal_type,
            timestamp,
            price_at_alert,
            change_1h_pct,
            is_success,
            strftime('%H', timestamp) as hour_of_day,
            strftime('%w', timestamp) as day_of_week
        FROM alerts
        WHERE analyzed = 1
    ''')
    
    alerts = cursor.fetchall()
    conn.close()
    
    if not alerts:
        print("⏳ 暂无数据，继续积累中...")
        return
    
    total = len(alerts)
    success_count = sum(1 for a in alerts if a[5])
    success_rate = success_count / total * 100 if total > 0 else 0
    
    print("\n" + "="*60)
    print("📊 警报历史分析报告")
    print("="*60)
    print(f"📁 数据总量：{total} 条警报")
    print(f"✅ 成功数：{success_count} ({success_rate:.1f}%)")
    print(f"❌ 失败数：{total - success_count} ({100-success_rate:.1f}%)")
    
    # 1. 按信号类型分析
    print("\n" + "="*60)
    print("📈 按信号类型:")
    print("="*60)
    
    by_type = {}
    for a in alerts:
        signal_type = a[1]
        if signal_type not in by_type:
            by_type[signal_type] = {'total': 0, 'success': 0}
        by_type[signal_type]['total'] += 1
        if a[5]:
            by_type[signal_type]['success'] += 1
    
    for signal_type, stats in sorted(by_type.items(), key=lambda x: x[1]['success']/x[1]['total'] if x[1]['total'] > 0 else 0, reverse=True):
        rate = stats['success'] / stats['total'] * 100 if stats['total'] > 0 else 0
        print(f"  {signal_type:<15} {stats['success']:>3}/{stats['total']:<3} ({rate:>5.1f}%)")
    
    # 2. 按币种分析 (Top 10)
    print("\n" + "="*60)
    print("🪙 按币种 (Top 10):")
    print("="*60)
    
    by_symbol = {}
    for a in alerts:
        symbol = a[0]
        if symbol not in by_symbol:
            by_symbol[symbol] = {'total': 0, 'success': 0}
        by_symbol[symbol]['total'] += 1
        if a[5]:
            by_symbol[symbol]['success'] += 1
    
    # 只统计至少有 3 次警报的币种
    qualified = {k: v for k, v in by_symbol.items() if v['total'] >= 3}
    sorted_symbols = sorted(qualified.items(), key=lambda x: x[1]['success']/x[1]['total'] if x[1]['total'] > 0 else 0, reverse=True)[:10]
    
    for symbol, stats in sorted_symbols:
        rate = stats['success'] / stats['total'] * 100 if stats['total'] > 0 else 0
        print(f"  {symbol:<15} {stats['success']:>3}/{stats['total']:<3} ({rate:>5.1f}%)")
    
    # 3. 按时间段分析
    print("\n" + "="*60)
    print("⏰ 按时间段:")
    print("="*60)
    
    by_hour = {}
    for a in alerts:
        hour = a[6]
        if hour not in by_hour:
            by_hour[hour] = {'total': 0, 'success': 0}
        by_hour[hour]['total'] += 1
        if a[5]:
            by_hour[hour]['success'] += 1
    
    for hour in sorted(by_hour.keys()):
        stats = by_hour[hour]
        rate = stats['success'] / stats['total'] * 100 if stats['total'] > 0 else 0
        bar = "█" * int(rate / 5)
        print(f"  {hour}:00  {stats['success']:>3}/{stats['total']:<3} ({rate:>5.1f}%) {bar}")
    
    # 4. 成功警报的特征
    print("\n" + "="*60)
    print("🎯 成功警报特征:")
    print("="*60)
    
    success_alerts = [a for a in alerts if a[5]]
    if success_alerts:
        avg_change = sum(a[4] for a in success_alerts) / len(success_alerts)
        print(f"  平均涨幅：{avg_change:+.1f}%")
        
        # 最常见信号
        signal_counts = {}
        for a in success_alerts:
            signal_counts[a[1]] = signal_counts.get(a[1], 0) + 1
        most_common_signal = max(signal_counts.items(), key=lambda x: x[1])[0]
        print(f"  最常见信号：{most_common_signal}")
        
        # 最常见时段
        hour_counts = {}
        for a in success_alerts:
            hour_counts[a[6]] = hour_counts.get(a[6], 0) + 1
        most_common_hour = max(hour_counts.items(), key=lambda x: x[1])[0]
        print(f"  最常见时段：{most_common_hour}:00")
        
        # 高频成功币种
        symbol_counts = {}
        for a in success_alerts:
            symbol_counts[a[0]] = symbol_counts.get(a[0], 0) + 1
        top_symbols = sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        if top_symbols:
            print(f"  高频成功币种：{', '.join([s[0] for s in top_symbols])}")
    
    # 5. 最近成功案例
    print("\n" + "="*60)
    print("🔥 最近成功案例 (Top 10):")
    print("="*60)
    
    cursor = conn = sqlite3.connect('/root/CryptoMonitor/alerts.db').cursor()
    cursor.execute('''
        SELECT symbol, signal_type, timestamp, price_at_alert, change_1h_pct
        FROM alerts
        WHERE is_success = 1
        ORDER BY timestamp DESC
        LIMIT 10
    ''')
    recent_success = cursor.fetchall()
    conn.close()
    
    print(f"{'币种':<15} {'信号':<12} {'时间':<20} {'警报价':<12} {'1h 后':<10}")
    print("-" * 70)
    for row in recent_success:
        symbol, signal_type, timestamp, price, change = row
        print(f"{symbol:<15} {signal_type:<12} {timestamp:<20} ${price:<12.6f} {change:+.1f}%")
    
    print("\n" + "="*60)
    print("📁 报告完成")
    print("="*60)

if __name__ == '__main__':
    analyze_success_rate()
