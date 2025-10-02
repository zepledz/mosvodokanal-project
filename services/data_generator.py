# data_generator.py
import json
import random
import time
import logging
import uuid
from datetime import datetime, timedelta
from confluent_kafka import Producer
import threading
from models import WaterMeasurement, BuildingStats, ITP, MKD, get_db, init_db
from sqlalchemy.orm import Session
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataGenerator:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        logger.info(f"🔌 Подключение к Kafka: {self.bootstrap_servers}")
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})

        self.buildings = self.generate_buildings(50)
        self.running = False
        self.thread = None

        # Инициализация базы данных
        self.db_session = None
        self.setup_database()

    def setup_database(self):
        """Инициализация подключения к базе данных"""
        try:
            init_db()
            self.db_session = next(get_db())
            logger.info("✅ Подключение к базе данных установлено")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к базе данных: {e}")
            self.db_session = None

    def generate_buildings(self, count):
        """Генерирует список зданий с привязкой к ИТП"""
        buildings = []
        moscow_districts = [
            "ЦАО", "САО", "СВАО", "ВАО", "ЮВАО",
            "ЮАО", "ЮЗАО", "ЗАО", "СЗАО", "НАО"
        ]

        for i in range(count):
            district = random.choice(moscow_districts)
            itp_id = f"itp_{random.randint(1, 5)}"  # Привязываем к одному из 5 ИТП

            buildings.append({
                'building_id': f"bld_{i + 1}",
                'address': f"г. Москва, {district}, ул. Примерная, д. {random.randint(1, 100)}",
                'fias_id': str(uuid.uuid4()),
                'unom': str(random.randint(10000, 99999)),
                'device_id': f"dev_{i + 1:03d}",
                'itp_id': itp_id,
                'hvs_water_meter_id': f"hvs_meter_{i + 1:04d}",
                'gvs_flowmeter1_id': f"gvs1_meter_{i + 1:04d}",
                'gvs_flowmeter2_id': f"gvs2_meter_{i + 1:04d}"
            })
        return buildings

    def generate_scenario(self):
        """Генерирует сценарий потребления воды"""
        scenarios = {
            'normal': (0.5, 2.0, 0.3, 1.5, 0, 0.5),  # hvs_min, hvs_max, gvs1_min, gvs1_max, gvs2_min, gvs2_max
            'high_consumption': (2.0, 5.0, 1.5, 4.0, 0.1, 1.0),
            'low_consumption': (0.1, 1.0, 0.1, 0.8, 0, 0.2),
            'anomaly_hvs': (5.0, 10.0, 0.3, 1.5, 0, 0.3),  # Аномально высокое ХВС
            'anomaly_gvs': (0.5, 2.0, 3.0, 8.0, 0.5, 2.0),  # Аномально высокое ГВС
            'zero_consumption': (0.0, 0.1, 0.0, 0.1, 0.0, 0.05)
        }

        return random.choices(
            list(scenarios.keys()),
            weights=[0.6, 0.15, 0.1, 0.05, 0.05, 0.05],
            k=1
        )[0]

    def generate_measurement(self, building):
        """Генерирует одно измерение с двумя каналами ГВС"""
        scenario = self.generate_scenario()
        scenarios = {
            'normal': (0.5, 2.0, 0.3, 1.5, 0, 0.5),
            'high_consumption': (2.0, 5.0, 1.5, 4.0, 0.1, 1.0),
            'low_consumption': (0.1, 1.0, 0.1, 0.8, 0, 0.2),
            'anomaly_hvs': (5.0, 10.0, 0.3, 1.5, 0, 0.3),
            'anomaly_gvs': (0.5, 2.0, 3.0, 8.0, 0.5, 2.0),
            'zero_consumption': (0.0, 0.1, 0.0, 0.1, 0.0, 0.05)
        }

        hvs_min, hvs_max, gvs1_min, gvs1_max, gvs2_min, gvs2_max = scenarios[scenario]

        # Генерируем значения
        hvs_value = round(random.uniform(hvs_min, hvs_max), 3)
        gvs_flow1_value = round(random.uniform(gvs1_min, gvs1_max), 3)
        gvs_flow2_value = round(random.uniform(gvs2_min, gvs2_max), 3)
        gvs_total = round(gvs_flow1_value - gvs_flow2_value, 3)

        data = {
            'measurement_id': str(uuid.uuid4()),
            'device_id': building['device_id'],  # Убедимся что device_id заполнен
            'building_id': building['building_id'],
            'itp_id': building['itp_id'],  # Добавляем itp_id
            'address': building['address'],
            'fias_id': building['fias_id'],
            'unom': building['unom'],

            # ХВС данные
            'hvs_water_meter_id': building['hvs_water_meter_id'],
            'hvs_value': hvs_value,

            # ГВС данные (два канала)
            'gvs_flowmeter1_id': building['gvs_flowmeter1_id'],
            'gvs_flow1_value': gvs_flow1_value,
            'gvs_flowmeter2_id': building['gvs_flowmeter2_id'],
            'gvs_flow2_value': gvs_flow2_value,
            'gvs_total': gvs_total,

            'scenario': scenario,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }

        # Отправляем в Kafka
        self.producer.produce('water_measurements', json.dumps(data))
        self.producer.poll(0)

        # Сохраняем в базу данных
        self.save_to_database(data)

        return data
    def save_to_database(self, data):
        """Сохраняет данные в базу данных с новой структурой"""
        if not self.db_session:
            logger.warning("❌ Нет подключения к базе данных")
            return

        try:
            # Убедимся что device_id не None
            device_id = data.get('device_id')
            if not device_id:
                logger.warning(f"⚠️ Пропуск записи с пустым device_id: {data['building_id']}")
                return

            # Расчет отклонения между ХВС и ГВС
            hvs_value = float(data['hvs_value'])
            gvs_total = float(data['gvs_total'])

            if hvs_value > 0:
                deviation = abs((gvs_total / hvs_value) - 1) * 100
            else:
                deviation = 0

            is_anomaly = deviation > 10

            # Создаем запись измерения с новыми полями
            measurement = WaterMeasurement(
                measurement_id=data['measurement_id'],
                building_id=data['building_id'],
                address=data['address'],
                device_id=device_id,  # Используем проверенный device_id
                fias_id=data.get('fias_id'),
                unom=data.get('unom'),

                # ХВС данные
                hvs_water_meter_id=data['hvs_water_meter_id'],
                hvs_value=hvs_value,

                # ГВС данные (два канала)
                gvs_flowmeter1_id=data['gvs_flowmeter1_id'],
                gvs_flow1_value=float(data['gvs_flow1_value']),
                gvs_flowmeter2_id=data['gvs_flowmeter2_id'],
                gvs_flow2_value=float(data['gvs_flow2_value']),
                gvs_total=gvs_total,

                deviation=deviation,
                is_anomaly=is_anomaly,
                scenario=data['scenario'],
                measurement_timestamp=datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            )

            self.db_session.add(measurement)
            self.db_session.commit()

            # Обновляем статистику здания
            self.update_building_stats(data, deviation, is_anomaly)

            logger.debug(f"💾 Данные сохранены в базу для {data['address']}")

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в базу данных: {e}")
            self.db_session.rollback()

    def update_building_stats(self, data, deviation, is_anomaly):
        """Обновляет статистику по зданию - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            # СОЗДАЕМ НОВУЮ СЕССИЮ каждый раз
            from models import get_db
            db = next(get_db())

            building_id = data['building_id']
            stats = db.query(BuildingStats).filter_by(building_id=building_id).first()

            if not stats:
                stats = BuildingStats(
                    building_id=building_id,
                    address=data['address'],
                    itp_id=data.get('itp_id'),
                    total_measurements=0,
                    anomaly_count=0,
                    last_measurement=datetime.utcnow()
                )
                db.add(stats)
                logger.info(f"✅ Создана новая запись в building_stats для {building_id}")

            stats.total_measurements += 1
            if is_anomaly:
                stats.anomaly_count += 1
            stats.last_measurement = datetime.utcnow()
            stats.updated_at = datetime.utcnow()

            db.commit()
            logger.debug(f"📊 Обновлена статистика для {building_id}")

        except Exception as e:
            logger.error(f"❌ Ошибка обновления статистики для {data['building_id']}: {e}")
            if 'db' in locals():
                db.rollback()
        finally:
            if 'db' in locals():
                db.close()  # ВАЖНО: закрываем сессию

    def start_generation(self):
        """Запускает генерацию данных"""
        self.running = True
        self.thread = threading.Thread(target=self._generation_loop)
        self.thread.start()
        logger.info("🚀 Генератор данных запущен")

    def _generation_loop(self):
        """Цикл генерации данных"""
        while self.running:
            building = random.choice(self.buildings)
            measurement = self.generate_measurement(building)

            logger.info(f"📊 Сгенерировано измерение: {measurement['building_id']} | "
                        f"ХВС: {measurement['hvs_value']} м³ | "
                        f"ГВС1: {measurement['gvs_flow1_value']} м³ | "
                        f"ГВС2: {measurement['gvs_flow2_value']} м³ | "
                        f"ГВС total: {measurement['gvs_total']} м³ | "
                        f"Сценарий: {measurement['scenario']}")

            time.sleep(random.uniform(1, 3))

    def stop_generation(self):
        """Останавливает генерацию данных"""
        self.running = False
        if self.thread:
            self.thread.join()
        self.producer.flush()
        logger.info("🛑 Генератор данных остановлен")


if __name__ == "__main__":
    generator = DataGenerator()

    try:
        generator.start_generation()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        generator.stop_generation()