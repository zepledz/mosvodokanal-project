# models.py
import os
import uuid
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, DECIMAL, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import logging
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Подключение к базе данных
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://admin:password@localhost:5432/mosvodokanal')
engine = create_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=30,
    pool_pre_ping=True,
    pool_recycle=3600
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(80), unique=True, nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(50), default='dispatcher')
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def get_id(self):
        return str(self.id)
# НОВАЯ СУЩНОСТЬ: ИТП
class ITP(Base):
    __tablename__ = "itp"

    itp_id = Column(String(50), primary_key=True, default=lambda: str(uuid.uuid4()))
    itp_number = Column(String(100), nullable=False)  # Номер ИТП
    address = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


# НОВАЯ СУЩНОСТЬ: МКД с привязкой к ИТП
class MKD(Base):
    __tablename__ = "mkd"

    building_id = Column(String(50), primary_key=True)
    address = Column(Text, nullable=False)
    fias_id = Column(String(50))  # ФИАС - уже есть у вас
    unom = Column(String(50))  # УНОМ - уже есть у вас
    itp_id = Column(String(50), ForeignKey('itp.itp_id'))  # Связь с ИТП
    created_at = Column(DateTime, default=datetime.utcnow)


# ОБНОВЛЕННАЯ СУЩНОСТЬ: Измерения с поддержкой двух каналов ГВС
# models.py - ИСПРАВЛЕННЫЙ класс WaterMeasurement
class WaterMeasurement(Base):
    __tablename__ = "water_measurements"

    id = Column(Integer, primary_key=True, index=True)
    measurement_id = Column(String(50), nullable=False, default=lambda: str(uuid.uuid4()))
    device_id = Column(String(100), nullable=False)
    building_id = Column(String(50), nullable=False)
    address = Column(Text, nullable=False)
    fias_id = Column(String(50))
    unom = Column(String(50))

    # ХВС данные
    hvs_value = Column(DECIMAL(10, 3), nullable=False)
    hvs_water_meter_id = Column(String(100))

    # ГВС данные (два канала)
    gvs_flow1_value = Column(DECIMAL(10, 3), nullable=False)
    gvs_flow2_value = Column(DECIMAL(10, 3), default=0)
    gvs_total = Column(DECIMAL(10, 3))
    gvs_flowmeter1_id = Column(String(100))
    gvs_flowmeter2_id = Column(String(100))

    # Расчетные поля
    deviation = Column(DECIMAL(5, 2), default=0)
    is_anomaly = Column(Boolean, default=False)
    scenario = Column(String(20), nullable=False)

    # ИТП информация
    itp_id = Column(String(50), ForeignKey('itp.itp_id'))

    # Временные метки
    measurement_timestamp = Column(DateTime, nullable=False)
    received_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)


# ОБНОВЛЕННАЯ СУЩНОСТЬ: Статистика зданий
class BuildingStats(Base):
    __tablename__ = "building_stats"

    building_id = Column(String(50), primary_key=True)
    address = Column(Text, nullable=False)
    total_measurements = Column(Integer, default=0)
    anomaly_count = Column(Integer, default=0)
    avg_hvs_value = Column(DECIMAL(10, 3))
    avg_gvs_value = Column(DECIMAL(10, 3))
    last_measurement = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.utcnow)
    fias_id = Column(String(50))  # Для совместимости
    unom = Column(String(50))     # Для совместимости
    # НОВОЕ: Привязка к ИТП
    itp_id = Column(String(50), ForeignKey('itp.itp_id'))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ База данных инициализирована")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации базы данных: {e}")


if __name__ == "__main__":
    init_db()