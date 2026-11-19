"""ClickHouse 存储共享常量与日志器。"""

from __future__ import annotations

import logging

logger = logging.getLogger("app.storage.store")

# 全市场大跨度扫描用 argMax（§4.3）；有 symbol 过滤时 FINAL 更快。
ARGMAX_SPAN_DAYS = 180
