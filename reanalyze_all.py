#!/usr/bin/env python3
"""
批量重新分析所有历史警报
每批 200 条，避免超时
"""

import subprocess
import time

BATCH_SIZE = 200
MAX_BATCHES = 40  # 最多处理 8000 条

print("🔄 开始批量重新分析所有历史警报...\n")

for i in range(1, MAX_BATCHES + 1):
    print(f"\n{'='*60}")
    print(f"📦 批次 {i}/{MAX_BATCHES}")
    print(f"{'='*60}\n")
    
    # 运行分析
    result = subprocess.run(
        ['python3', 'update_alert_outcomes.py', str(BATCH_SIZE)],
        capture_output=True,
        text=True,
        timeout=90
    )
    
    # 显示结果
    lines = result.stdout.strip().split('\n')
    for line in lines[-6:]:  # 显示最后 6 行
        print(line)
    
    # 检查是否还有数据
    if "已更新 0/0 条" in result.stdout or "暂无待分析的警报" in result.stdout:
        print("\n✅ 所有警报已分析完成！")
        break
    
    # 间隔 1 秒
    time.sleep(1)

print("\n" + "="*60)
print("✅ 批量处理完成！")
print("="*60)
