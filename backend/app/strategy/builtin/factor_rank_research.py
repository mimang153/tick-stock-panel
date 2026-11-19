"""兼容转发: 研究模板已迁移至 app.strategy.research.factor_rank_research。"""

import sys

from app.strategy.research import factor_rank_research as _impl

sys.modules[__name__] = _impl
