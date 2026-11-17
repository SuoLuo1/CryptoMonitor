CryptoMonitor 当前 Top 首次信号 -> 后续最高价回测
============================================================
统计时点 UTC+8：2026-10-24 00:49:10
Top 数量：100
成功计算后续最高价：100
K 线失败币种：0
当前 Top 显示起点的平均 long ROI to high：33.6534%
当前 Top 显示起点的中位数 long ROI to high：3.2427%
连续段真实起点的平均 long ROI to high：33.6534%
连续段真实起点的中位数 long ROI to high：3.2427%
有明确方向首信号的 Top 显示起点样本：98
方向性 MFE 均值：26.3988%
方向性 MFE 中位数：2.6270%

重要口径：
1. Top 名单和排名顺序尽量逐字复现 utils.py 的 get_today_symbols_top()，不是指定一段历史 since/until。
2. top_display_* 是当前 Top 实际显示的 first_ts/first_price 后的结果。
3. segment_* 是按 Top 函数注释的意图计算的连续信号段起点后结果；若两者不同，说明 Top 源码可能将 first_ts 错设为最新一条。
4. long ROI to high=(信号后最高价/首次信号价-1)*100，是事后最大上行空间，不是实际可实现收益。
5. 方向性 MFE：首信号为 LONG 看后续最高价；为 SHORT 则看首信号价到后续最低价的空头有利幅度；OI/未知方向不计算方向性 MFE。
6. 信号所在分钟被排除，避免把报警前的分钟内最高/最低价纳入；可能保守漏掉报警后同一分钟内的极值。
7. Binance 1d K 线按 UTC 日切分；信号所在 UTC 日使用信号分钟之后的 1m K 线，后续 UTC 日才使用 1d high/low。
8. high/low 来自后续 1d 的，如果极值日 1m 数据成功取得，会定位极值分钟；否则只给 UTC 日，不伪造准确分钟。
9. current_top_snapshot.csv 保存本次 Top 名单快照；current_top_high_roi.csv 是逐币结果；current_top_errors.csv 记录失败交易对。
10. K 线缓存单独放置，不修改 alerts.db；再次运行会复用已下载的 K 线。
