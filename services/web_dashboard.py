# web_dashboard.py
import os
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO
from confluent_kafka import Consumer, KafkaError
import json
import threading
import logging
from datetime import datetime, timedelta
import redis
from models import WaterMeasurement, BuildingStats, get_db, init_db, ITP, MKD
from sqlalchemy import desc, func
import time
from ml_predictor import initialize_ml_predictor, get_ml_predictor
from migrations import run_migrations
from flask_cors import CORS


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'mosvodokanal-secret-key-2024'

CORS(app,
     supports_credentials=True,
     origins=["http://localhost:5000", "http://localhost:3000", "http://127.0.0.1:5000"])

# Инициализация аутентификации
from auth import auth_bp, login_manager
login_manager.init_app(app)
app.register_blueprint(auth_bp)

socketio = SocketIO(app,
                   cors_allowed_origins="*",
                   async_mode='threading',
                   logger=True,
                   engineio_logger=True,
                   ping_timeout=60,
                   ping_interval=25)

redis_host = os.getenv('REDIS_HOST', 'redis')
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)

database_url = os.getenv('DATABASE_URL', 'postgresql://admin:password@postgres:5433/mosvodokanal')
initialize_ml_predictor(database_url)
run_migrations()

class DataProcessor:
    def __init__(self):
        logger.info(f"🔌 Подключение к Kafka: {kafka_bootstrap_servers}")

        kafka_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'web_dashboard_group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
        }

        try:
            self.kafka_consumer = Consumer(kafka_config)
            self.kafka_consumer.subscribe(['water_measurements'])
            logger.info("✅ Kafka Consumer успешно создан и подписан на топик")
        except Exception as e:
            logger.error(f"❌ Ошибка создания Kafka Consumer: {e}")
            self.kafka_consumer = None

        self.setup_database()

    def setup_database(self):
        """Инициализация подключения к базе данных"""
        try:
            init_db()
            logger.info("✅ Web Dashboard: Подключение к базе данных установлено")
        except Exception as e:
            logger.error(f"❌ Web Dashboard: Ошибка подключения к базе данных: {e}")

    def start_consuming(self):
        """Запускает потребление данных из Kafka"""
        if self.kafka_consumer:
            threading.Thread(target=self._consume_loop, daemon=True).start()
            logger.info("🚀 Потребитель Kafka запущен")
        else:
            logger.warning("⚠️ Потребитель Kafka не запущен из-за ошибки инициализации")

    def _consume_loop(self):
        """Цикл потребления данных из Kafka"""
        logger.info("🔄 Начало цикла потребления Kafka...")
        while True:
            try:
                if not self.kafka_consumer:
                    logger.error("❌ Kafka Consumer не инициализирован")
                    time.sleep(5)
                    continue

                msg = self.kafka_consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"❌ Ошибка Kafka: {msg.error()}")
                        time.sleep(1)
                        continue

                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"📨 Получено сообщение из Kafka: {data['building_id']}")

                    hvs_value = float(data['hvs_value'])
                    gvs_flow1_value = float(data['gvs_flow1_value'])
                    gvs_flow2_value = float(data['gvs_flow2_value'])
                    gvs_total = float(data['gvs_total'])

                    deviation, calculated_gvs_total = calculate_deviation(
                        hvs_value, gvs_flow1_value, gvs_flow2_value, "circulation"
                    )

                    status = "🚨" if deviation > 10 else "✅"
                    anomaly_type = ""
                    if deviation > 10:
                        if gvs_total > hvs_value:
                            anomaly_type = " (Высокое ГВС)"
                        else:
                            anomaly_type = " (Высокое ХВС)"

                    processed_data = {
                        "address": data['address'],
                        "cold_water": hvs_value,
                        "hot_water": gvs_total,
                        "gvs_flow1": gvs_flow1_value,
                        "gvs_flow2": gvs_flow2_value,
                        "deviation": round(deviation, 2),
                        "status": status + anomaly_type,
                        "timestamp": data['timestamp'],
                        "building_id": data['building_id'],
                        "itp_id": data.get('itp_id', ''),
                        "device_id": data.get('device_id', ''),
                        "scenario": data['scenario']
                    }

                    socketio.emit('new_measurement', processed_data)
                    logger.info(f"📤 Отправлено через WebSocket: {data['building_id']}")

                    redis_key = f"last_measurement:{data['building_id']}"
                    redis_client.setex(redis_key, 300, json.dumps(processed_data))

                    logger.debug(f"📥 Обработано измерение: {data['address']} | Отклонение: {deviation:.1f}%")

                except Exception as e:
                    logger.error(f"❌ Ошибка обработки сообщения: {e}")

            except Exception as e:
                logger.error(f"❌ Критическая ошибка в цикле потребления Kafka: {e}")
                time.sleep(5)

    def get_historical_data(self, limit=100):
        """Получает исторические данные из базы"""
        db = next(get_db())
        try:
            measurements = db.query(WaterMeasurement).order_by(
                desc(WaterMeasurement.measurement_timestamp)
            ).limit(limit).all()

            historical_data = []
            for measurement in measurements:
                historical_data.append({
                    "address": measurement.address,
                    "cold_water": float(measurement.hvs_value),
                    "hot_water": float(measurement.gvs_total),
                    "gvs_flow1": float(measurement.gvs_flow1_value),
                    "gvs_flow2": float(measurement.gvs_flow2_value),
                    "deviation": float(measurement.deviation),
                    "status": "🚨" if measurement.is_anomaly else "✅",
                    "timestamp": measurement.measurement_timestamp.isoformat(),
                    "building_id": measurement.building_id,
                    "itp_id": measurement.itp_id if measurement.itp_id else "",
                    "device_id": measurement.device_id,
                    "scenario": measurement.scenario
                })
            return historical_data
        except Exception as e:
            logger.error(f"❌ Ошибка получения исторических данных: {e}")
            return []
        finally:
            db.close()

    def get_database_stats(self):
        """Получает статистику из базы данных"""
        db = next(get_db())
        try:
            # Общая статистика
            total_measurements = db.query(WaterMeasurement).count()
            anomalies = db.query(WaterMeasurement).filter(WaterMeasurement.is_anomaly == True).count()
            total_buildings = db.query(BuildingStats).count()

            if total_measurements > 0:
                anomaly_percentage = (anomalies / total_measurements) * 100
            else:
                anomaly_percentage = 0

            return {
                "total_buildings": total_buildings,
                "total_measurements": total_measurements,
                "anomalies": anomalies,
                "normal": total_measurements - anomalies,
                "anomaly_percentage": round(anomaly_percentage, 1),
                "last_update": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики из базы: {e}")
            return {
                "total_buildings": 0,
                "total_measurements": 0,
                "anomalies": 0,
                "normal": 0,
                "anomaly_percentage": 0,
                "last_update": datetime.now().isoformat()
            }
        finally:
            db.close()

def calculate_deviation(hvs_flow_value, gvs_flow1_value, gvs_flow2_value, scheme_type="circulation"):
    """
    Расчет отклонения согласно ТЗ
    scheme_type: "circulation" (циркуляционная) или "dead_end" (тупиковая)
    """
    if scheme_type == "circulation":
        gvs_total = gvs_flow1_value - gvs_flow2_value
    else:
        gvs_total = gvs_flow1_value

    if hvs_flow_value > 0:
        deviation = abs((gvs_total - hvs_flow_value) / hvs_flow_value) * 100
    else:
        deviation = 0

    return deviation, gvs_total

def get_rule_based_predictions():
    """Rule-based прогнозы как fallback"""
    db = next(get_db())
    try:
        buildings = db.query(BuildingStats).order_by(
            desc(BuildingStats.last_measurement)
        ).all()

        predictions = []
        for building in buildings:
            latest_measurement = db.query(WaterMeasurement).filter(
                WaterMeasurement.building_id == building.building_id
            ).order_by(desc(WaterMeasurement.measurement_timestamp)).first()

            if latest_measurement:
                deviation = float(latest_measurement.deviation)
                risk_score = min(deviation / 50.0, 1.0)

                predictions.append({
                    "building_id": building.building_id,
                    "address": building.address,
                    "risk_score": round(risk_score, 3),
                    "risk_level": "высокий" if risk_score > 0.7 else "средний" if risk_score > 0.3 else "низкий",
                    "last_measurement": latest_measurement.measurement_timestamp.isoformat(),
                    "hvs_value": float(latest_measurement.hvs_value),
                    "gvs_value": float(latest_measurement.gvs_total),
                    "deviation": deviation,
                    "itp_id": building.itp_id
                })

        return jsonify({
            "predictions": predictions,
            "page": 1,
            "per_page": len(predictions),
            "total": len(predictions),
            "model_type": "rule_based_fallback"
        })
    except Exception as e:
        logger.error(f"❌ Ошибка rule-based прогнозов: {e}")
        return jsonify({"predictions": [], "total": 0, "error": str(e)})
    finally:
        db.close()

def get_rule_based_risk_summary():
    """Rule-based сводка рисков как fallback"""
    db = next(get_db())
    try:
        buildings = db.query(BuildingStats).all()

        high_risk = 0
        medium_risk = 0
        low_risk = 0

        for building in buildings:
            latest = db.query(WaterMeasurement).filter(
                WaterMeasurement.building_id == building.building_id
            ).order_by(desc(WaterMeasurement.measurement_timestamp)).first()

            if latest:
                deviation = float(latest.deviation)
                risk_score = min(deviation / 50.0, 1.0)

                if risk_score > 0.7:
                    high_risk += 1
                elif risk_score > 0.3:
                    medium_risk += 1
                else:
                    low_risk += 1

        return jsonify({
            "high_risk_buildings": high_risk,
            "medium_risk_buildings": medium_risk,
            "low_risk_buildings": low_risk,
            "total_buildings": len(buildings),
            "timestamp": datetime.now().isoformat(),
            "model_type": "rule_based_fallback"
        })
    except Exception as e:
        logger.error(f"❌ Ошибка rule-based сводки: {e}")
        return debug_risk_summary()
    finally:
        db.close()

processor = None

def initialize_processor():
    """Инициализация процессора данных"""
    global processor
    try:
        processor = DataProcessor()
        processor.start_consuming()
        logger.info("✅ DataProcessor успешно инициализирован")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации DataProcessor: {e}")
        processor = None

# Временные debug endpoints
@app.route('/api/debug/predictions')
def debug_predictions():
    """Временная заглушка для тестирования фронтенда"""
    mock_data = {
        "predictions": [
            {
                "building_id": "bld_001",
                "address": "ул. Тверская, д. 1",
                "risk_score": 0.85,
                "risk_level": "высокий",
                "last_measurement": datetime.now().isoformat(),
                "hvs_value": 15.234,
                "gvs_value": 8.567,
                "deviation": 45.2
            },
            {
                "building_id": "bld_002",
                "address": "ул. Арбат, д. 25",
                "risk_score": 0.45,
                "risk_level": "средний",
                "last_measurement": datetime.now().isoformat(),
                "hvs_value": 12.123,
                "gvs_value": 10.456,
                "deviation": 15.8
            },
            {
                "building_id": "bld_003",
                "address": "пр. Мира, д. 10",
                "risk_score": 0.15,
                "risk_level": "низкий",
                "last_measurement": datetime.now().isoformat(),
                "hvs_value": 9.876,
                "gvs_value": 9.123,
                "deviation": 5.2
            }
        ],
        "page": 1,
        "per_page": 10,
        "total": 3
    }
    return jsonify(mock_data)

@app.route('/api/force_save_model', methods=['POST'])
def force_save_model():
    """Принудительное сохранение модели"""
    try:
        ml_predictor = get_ml_predictor()
        if ml_predictor:
            success = ml_predictor.save_model()
            return jsonify({"status": "success" if success else "failed", "message": "Модель сохранена" if success else "Не удалось сохранить"})
        return jsonify({"status": "error", "message": "ML predictor не инициализирован"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/debug/ml_status')
def debug_ml_status():
    """Детальная диагностика ML модели"""
    try:
        ml_predictor = get_ml_predictor()

        status = {
            "ml_predictor_exists": ml_predictor is not None,
            "model_type": "ML" if ml_predictor and ml_predictor.model != "simple_rule_based" else "rule_based",
            "model_object_type": str(type(ml_predictor.model)) if ml_predictor and ml_predictor.model else "None",
            "model_file_exists": False,
            "error": None
        }

        if ml_predictor and hasattr(ml_predictor, 'model_path'):
            status["model_file_exists"] = os.path.exists(ml_predictor.model_path)
            status["model_path"] = ml_predictor.model_path

        return jsonify(status)

    except Exception as e:
        return jsonify({"error": str(e)})
@app.route('/api/debug/risk_summary')
def debug_risk_summary():
    """Временная заглушка для сводки рисков"""
    mock_data = {
        "high_risk_buildings": 8,
        "medium_risk_buildings": 15,
        "low_risk_buildings": 27,
        "total_buildings": 50,
        "timestamp": datetime.now().isoformat()
    }
    return jsonify(mock_data)

@app.route('/api/predictions')
def get_all_predictions():
    """Прогнозы для всех зданий с использованием ML модели"""
    db = next(get_db())
    try:
        ml_predictor = get_ml_predictor()
        if not ml_predictor:
            logger.warning("ML predictor не инициализирован, используем rule-based подход")
            return get_rule_based_predictions()

        buildings = db.query(BuildingStats).order_by(
            desc(BuildingStats.last_measurement)
        ).all()

        predictions = []
        for building in buildings:
            latest_measurement = db.query(WaterMeasurement).filter(
                WaterMeasurement.building_id == building.building_id
            ).order_by(desc(WaterMeasurement.measurement_timestamp)).first()

            if latest_measurement:
                try:
                    risk_score = ml_predictor.predict_anomaly_risk({
                        'hvs_value': float(latest_measurement.hvs_value),
                        'gvs_value': float(latest_measurement.gvs_total),  # Используем gvs_total
                        'deviation': float(latest_measurement.deviation),
                        'timestamp': latest_measurement.measurement_timestamp.isoformat(),
                        'building_id': building.building_id
                    })
                except Exception as e:
                    logger.warning(f"Ошибка ML прогноза для {building.building_id}, используем rule-based: {e}")
                    risk_score = min(float(latest_measurement.deviation) / 50.0, 1.0)

                predictions.append({
                    "building_id": building.building_id,
                    "address": building.address,
                    "risk_score": round(risk_score, 3),
                    "risk_level": "высокий" if risk_score > 0.7 else "средний" if risk_score > 0.3 else "низкий",
                    "last_measurement": latest_measurement.measurement_timestamp.isoformat(),
                    "hvs_value": float(latest_measurement.hvs_value),
                    "gvs_value": float(latest_measurement.gvs_total),
                    "deviation": float(latest_measurement.deviation),
                    "itp_id": building.itp_id
                })

        return jsonify({
            "predictions": predictions,
            "page": 1,
            "per_page": len(predictions),
            "total": len(predictions),
            "model_type": "ml" if ml_predictor and ml_predictor.model != "simple_rule_based" else "rule_based"
        })

    except Exception as e:
        logger.error(f"❌ Ошибка получения прогнозов: {e}")
        return get_rule_based_predictions()
    finally:
        db.close()

@app.route('/api/risk_summary')
def get_risk_summary():
    """Сводка по рискам с использованием ML модели"""
    db = next(get_db())
    try:
        ml_predictor = get_ml_predictor()
        if not ml_predictor:
            logger.warning("ML predictor не инициализирован, используем rule-based подход")
            return get_rule_based_risk_summary()

        buildings = db.query(BuildingStats).all()

        high_risk = 0
        medium_risk = 0
        low_risk = 0

        for building in buildings:
            latest = db.query(WaterMeasurement).filter(
                WaterMeasurement.building_id == building.building_id
            ).order_by(desc(WaterMeasurement.measurement_timestamp)).first()

            if latest:
                try:
                    risk_score = ml_predictor.predict_anomaly_risk({
                        'hvs_value': float(latest.hvs_value),
                        'gvs_value': float(latest.gvs_total),
                        'deviation': float(latest.deviation),
                        'timestamp': latest.measurement_timestamp.isoformat(),
                        'building_id': building.building_id
                    })
                except Exception as e:
                    logger.warning(f"Ошибка ML прогноза для {building.building_id}, используем rule-based: {e}")
                    risk_score = min(float(latest.deviation) / 50.0, 1.0)

                if risk_score > 0.7:
                    high_risk += 1
                elif risk_score > 0.3:
                    medium_risk += 1
                else:
                    low_risk += 1

        return jsonify({
            "high_risk_buildings": high_risk,
            "medium_risk_buildings": medium_risk,
            "low_risk_buildings": low_risk,
            "total_buildings": len(buildings),
            "timestamp": datetime.now().isoformat(),
            "model_type": "ml" if ml_predictor and ml_predictor.model != "simple_rule_based" else "rule_based"
        })

    except Exception as e:
        logger.error(f"❌ Ошибка получения сводки рисков: {e}")
        return get_rule_based_risk_summary()
    finally:
        db.close()

@app.route('/api/predictions/<building_id>')
def get_predictions(building_id):
    """Получить прогнозы для здания"""
    db = next(get_db())
    try:
        ml_predictor = get_ml_predictor()
        if not ml_predictor:
            return jsonify({"error": "ML predictor не инициализирован"})

        measurements = db.query(WaterMeasurement).filter(
            WaterMeasurement.building_id == building_id
        ).order_by(desc(WaterMeasurement.measurement_timestamp)).limit(100).all()

        if not measurements:
            return jsonify({"error": "Нет данных для прогноза"})

        latest = measurements[0]
        risk_score = ml_predictor.predict_anomaly_risk({
            'hvs_value': float(latest.hvs_value),
            'gvs_value': float(latest.gvs_total),
            'deviation': float(latest.deviation),
            'timestamp': latest.measurement_timestamp.isoformat(),
            'building_id': latest.building_id
        })

        recommendation = generate_recommendation(risk_score, latest)

        return jsonify({
            "building_id": building_id,
            "risk_score": round(risk_score, 3),
            "risk_level": "высокий" if risk_score > 0.7 else "средний" if risk_score > 0.3 else "низкий",
            "recommendation": recommendation,
            "timestamp": datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"❌ Ошибка прогнозирования: {e}")
        return jsonify({"error": "Ошибка прогнозирования"})
    finally:
        db.close()

def generate_recommendation(risk_score, measurement):
    """Генерация рекомендаций для диспетчера"""
    if risk_score > 0.7:
        return "🚨 ВЫСОКИЙ РИСК! Рекомендуется немедленная проверка оборудования и возможная утечка"
    elif risk_score > 0.5:
        return "⚠️ Повышенный риск. Рекомендуется усиленный мониторинг и планирование проверки"
    elif measurement.deviation > 15:
        return "📊 Отклонение превышает норму. Рекомендуется анализ причин расхождения"
    else:
        return "✅ Система работает в штатном режиме"

@socketio.on('connect')
def handle_connect():
    logger.info('✅ Клиент подключился через WebSocket')
    try:
        keys = redis_client.keys('last_measurement:*')
        for key in keys[-10:]:
            data = redis_client.get(key)
            if data:
                socketio.emit('new_measurement', json.loads(data))
        logger.info(f"📤 Отправлено {len(keys[-10:])} исторических записей новому клиенту")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки исторических данных: {e}")

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('❌ Клиент отключился от WebSocket')

@app.route('/api/buildings')
def get_buildings():
    """API для получения списка зданий"""
    db = next(get_db())
    try:
        buildings = db.query(BuildingStats).all()

        building_list = []
        for building in buildings:
            building_data = {
                "building_id": building.building_id,
                "address": building.address,
                "itp_id": getattr(building, 'itp_id', 'N/A'),
                "total_measurements": building.total_measurements,
                "anomaly_count": building.anomaly_count,
                "anomaly_percentage": round((building.anomaly_count / building.total_measurements * 100), 1) if building.total_measurements > 0 else 0,
                "last_measurement": building.last_measurement.isoformat() if building.last_measurement else None
            }
            building_list.append(building_data)

        return jsonify(building_list)
    except Exception as e:
        logger.error(f"❌ Ошибка получения списка зданий: {e}")
        return jsonify([])
    finally:
        db.close()

@app.route('/api/train_model', methods=['GET', 'POST'])
def train_model():
    """Принудительное обучение ML модели"""
    try:
        ml_predictor = get_ml_predictor()
        if not ml_predictor:
            return jsonify({"status": "error", "message": "ML predictor не инициализирован"})

        success = ml_predictor.train_model()

        if success:
            model_type = "ML" if ml_predictor.model != "simple_rule_based" else "rule-based"
            return jsonify({
                "status": "success",
                "message": f"Модель успешно обучена ({model_type})",
                "model_type": model_type
            })
        else:
            return jsonify({"status": "error", "message": "Не удалось обучить модель"})

    except Exception as e:
        logger.error(f"❌ Ошибка обучения модели: {e}")
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/model_status')
def model_status():
    """Статус ML модели"""
    ml_predictor = get_ml_predictor()
    status = {
        "initialized": ml_predictor is not None,
        "model_trained": ml_predictor is not None and ml_predictor.model is not None,
        "timestamp": datetime.now().isoformat()
    }
    return jsonify(status)

@app.route('/')
def index():
    """Главная страница"""
    return render_template('index.html')

@app.route('/api/stats')
def get_stats():
    """API для получения статистики"""
    if processor:
        stats = processor.get_database_stats()
        return jsonify(stats)
    else:
        return jsonify({
            "error": "DataProcessor не инициализирован",
            "total_buildings": 0,
            "total_measurements": 0,
            "anomalies": 0,
            "normal": 0,
            "anomaly_percentage": 0,
            "last_update": datetime.now().isoformat()
        })

@app.route('/api/historical')
def get_historical_data():
    """API для получения исторических данных"""
    limit = request.args.get('limit', 100, type=int)
    if processor:
        historical_data = processor.get_historical_data(limit)
        return jsonify(historical_data)
    else:
        return jsonify([])

@app.route('/api/measurements/<building_id>')
def get_building_measurements(building_id):
    """API для получения измерений конкретного здания"""
    db = next(get_db())
    try:
        limit = request.args.get('limit', 50, type=int)
        measurements = db.query(WaterMeasurement).filter(
            WaterMeasurement.building_id == building_id
        ).order_by(desc(WaterMeasurement.measurement_timestamp)).limit(limit).all()

        measurement_list = []
        for measurement in measurements:
            measurement_list.append({
                "timestamp": measurement.measurement_timestamp.isoformat(),
                "hvs_value": float(measurement.hvs_value),
                "gvs_flow1_value": float(measurement.gvs_flow1_value),
                "gvs_flow2_value": float(measurement.gvs_flow2_value),
                "gvs_total": float(measurement.gvs_total),
                "deviation": float(measurement.deviation),
                "is_anomaly": measurement.is_anomaly,
                "scenario": measurement.scenario
            })

        return jsonify(measurement_list)
    except Exception as e:
        logger.error(f"❌ Ошибка получения измерений здания: {e}")
        return jsonify([])
    finally:
        db.close()

@app.route('/health')
def health_check():
    """Health check endpoint"""
    status = {
        "status": "healthy",
        "kafka_connected": processor is not None and processor.kafka_consumer is not None,
        "database_connected": True,
        "redis_connected": redis_client.ping() if redis_client else False,
        "timestamp": datetime.now().isoformat()
    }
    return jsonify(status)

@app.route('/api/charts/consumption')
def get_consumption_chart():
    """Данные для графика потребления"""
    db = next(get_db())
    try:
        period = request.args.get('period', 30, type=int)
        building_id = request.args.get('building_id', 'all')

        query = db.query(WaterMeasurement)

        start_date = datetime.now() - timedelta(days=period)
        query = query.filter(WaterMeasurement.measurement_timestamp >= start_date)

        if building_id != 'all':
            query = query.filter(WaterMeasurement.building_id == building_id)

        measurements = query.all()

        if not measurements:
            return jsonify({"error": "Нет данных за выбранный период"})

        total_hvs = sum(float(m.hvs_value) for m in measurements)
        total_gvs = sum(float(m.gvs_total) for m in measurements)
        anomalies = sum(1 for m in measurements if m.is_anomaly)
        normal = len(measurements) - anomalies

        return jsonify({
            "consumption": {
                "hvs": round(total_hvs, 2),
                "gvs": round(total_gvs, 2)
            },
            "anomalies": {
                "anomalies": anomalies,
                "normal": normal,
                "total": len(measurements)
            }
        })

    except Exception as e:
        logger.error(f"❌ Ошибка получения данных для графиков: {e}")
        return jsonify({"error": "Ошибка получения данных"})
    finally:
        db.close()

@app.route('/api/charts/trend')
def get_trend_chart():
    """Данные для графика тренда"""
    db = next(get_db())
    try:
        period = request.args.get('period', 30, type=int)
        building_id = request.args.get('building_id', 'all')

        dates = []
        for i in range(period, 0, -1):
            date = datetime.now() - timedelta(days=i)
            dates.append(date.strftime('%Y-%m-%d'))

        daily_data = []

        for single_date in [(datetime.now() - timedelta(days=i)) for i in range(period, 0, -1)]:
            next_date = single_date + timedelta(days=1)

            query = db.query(WaterMeasurement).filter(
                WaterMeasurement.measurement_timestamp >= single_date,
                WaterMeasurement.measurement_timestamp < next_date
            )

            if building_id != 'all':
                query = query.filter(WaterMeasurement.building_id == building_id)

            measurements = query.all()

            day_hvs = sum(float(m.hvs_value) for m in measurements)
            day_gvs = sum(float(m.gvs_total) for m in measurements)  # Используем gvs_total
            day_anomalies = sum(1 for m in measurements if m.is_anomaly)

            daily_data.append({
                "date": single_date.strftime('%m-%d'),
                "hvs": round(day_hvs, 2),
                "gvs": round(day_gvs, 2),
                "anomalies": day_anomalies
            })

        return jsonify({"daily_data": daily_data})

    except Exception as e:
        logger.error(f"❌ Ошибка получения трендовых данных: {e}")
        return jsonify({"error": "Ошибка получения данных"})
    finally:
        db.close()

@app.route('/api/charts/buildings')
def get_buildings_for_charts():
    """Список зданий для фильтров"""
    db = next(get_db())
    try:
        buildings = db.query(BuildingStats).all()

        building_list = [{"id": "all", "address": "Все здания"}]
        for building in buildings:
            building_list.append({
                "id": building.building_id,
                "address": building.address
            })

        return jsonify({"buildings": building_list})

    except Exception as e:
        logger.error(f"❌ Ошибка получения списка зданий: {e}")
        return jsonify({"error": "Ошибка получения данных"})
    finally:
        db.close()

@app.route('/api/itp')
def get_itp_list():
    """Список ИТП"""
    db = next(get_db())
    try:
        itp_list = db.query(ITP).all()
        return jsonify([{
            "itp_id": itp.itp_id,
            "itp_number": itp.itp_number,
            "created_at": itp.created_at.isoformat()
        } for itp in itp_list])
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        db.close()

@app.route('/api/mkd')
def get_mkd_list():
    """Список МКД с привязкой к ИТП"""
    db = next(get_db())
    try:
        mkd_list = db.query(MKD).all()
        return jsonify([{
            "building_id": mkd.building_id,
            "address": mkd.address,
            "fias": mkd.fias,
            "unom": mkd.unom,
            "itp_id": mkd.itp_id
        } for mkd in mkd_list])
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        db.close()

@app.route('/api/charts/risks')
def get_risk_chart():
    db = next(get_db())
    try:
        buildings = db.query(BuildingStats).limit(10).all()

        risk_data = []

        for building in buildings:
            latest = db.query(WaterMeasurement).filter(
                WaterMeasurement.building_id == building.building_id
            ).order_by(desc(WaterMeasurement.measurement_timestamp)).first()

            if latest:
                risk_level = min(float(latest.deviation) / 50.0, 1.0)
                risk_category = "высокий" if risk_level > 0.7 else "средний" if risk_level > 0.3 else "низкий"

                risk_data.append({
                    "address": building.address[:20] + "..." if len(building.address) > 20 else building.address,
                    "risk_score": round(risk_level * 100, 1),
                    "risk_category": risk_category,
                    "consumption": float(latest.hvs_value) + float(latest.gvs_total)
                })

        return jsonify({"risk_data": risk_data})

    except Exception as e:
        logger.error(f"❌ Ошибка получения данных рисков: {e}")
        return jsonify({"error": "Ошибка получения данных"})
    finally:
        db.close()


@app.route('/api/debug/save_model', methods=['POST'])
def debug_save_model():
    """Диагностика сохранения модели"""
    try:
        ml_predictor = get_ml_predictor()
        if not ml_predictor:
            return jsonify({"error": "ML predictor не инициализирован"})

        success = False
        if hasattr(ml_predictor, 'save_model'):
            success = ml_predictor.save_model()
            return jsonify({
                "status": "success" if success else "failed",
                "has_save_method": True,
                "model_type": str(type(ml_predictor.model)),
                "model_is_none": ml_predictor.model is None
            })
        else:
            return jsonify({
                "status": "error",
                "has_save_method": False,
                "message": "Метод save_model не найден"
            })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@app.route('/api/debug/model_info')
def debug_model_info():
    """Подробная информация о модели"""
    try:
        ml_predictor = get_ml_predictor()
        if not ml_predictor:
            return jsonify({"error": "ML predictor не инициализирован"})

        return jsonify({
            "model_exists": ml_predictor.model is not None,
            "model_type": str(type(ml_predictor.model)),
            "model_attributes": dir(ml_predictor.model) if ml_predictor.model else [],
            "has_save_method": hasattr(ml_predictor, 'save_model'),
            "model_path": getattr(ml_predictor, 'model_path', 'unknown')
        })
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    initialize_processor()

    logger.info("🚀 Запуск Flask приложения на порту 5000...")
    try:
        socketio.run(app,
                    host='0.0.0.0',
                    port=5000,
                    debug=False,
                    allow_unsafe_werkzeug=True)
    except Exception as e:
        logger.error(f"❌ Ошибка запуска Flask приложения: {e}")