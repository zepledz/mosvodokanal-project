# services/migrations.py
from models import engine
import logging

logger = logging.getLogger(__name__)


def run_migrations():
    """Запуск всех необходимых миграций"""
    migrations = [
        "ALTER TABLE building_stats ADD COLUMN IF NOT EXISTS itp_id VARCHAR(50)",
        "ALTER TABLE building_stats ADD COLUMN IF NOT EXISTS fias_id VARCHAR(50)",
        "ALTER TABLE building_stats ADD COLUMN IF NOT EXISTS unom VARCHAR(50)",
        "ALTER TABLE water_measurements ADD COLUMN IF NOT EXISTS gvs_flow1_value DECIMAL(10,3)",
        "ALTER TABLE water_measurements ADD COLUMN IF NOT EXISTS gvs_flow2_value DECIMAL(10,3)",
    ]

    with engine.connect() as conn:
        for migration in migrations:
            try:
                conn.execute(migration)
                logger.info(f"✅ Применена миграция: {migration}")
            except Exception as e:
                logger.error(f"❌ Ошибка миграции: {e}")