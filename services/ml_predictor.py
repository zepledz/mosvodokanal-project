# ml_predictor.py
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import joblib
from sqlalchemy import create_engine, text
import numpy as np
import logging
import os
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WaterConsumptionPredictor:
    def __init__(self, database_url):
        self.model = None
        self.database_url = database_url
        self.model_path = 'water_anomaly_model.pkl'
        self.label_encoders = {}
        self.load_model()

    def load_model(self):
        """Загрузка модели из файла, если существует"""
        try:
            # Загружаем модель и энкодеры
            model_data = joblib.load(self.model_path)
            self.model = model_data['model']
            self.label_encoders = model_data.get('label_encoders', {})
            logger.info("✅ ML модель загружена из файла")
        except Exception as e:
            logger.warning(f"❌ Не удалось загрузить модель: {e}. Будет обучена новая модель.")
            self.model = None
            self.label_encoders = {}

    def prepare_features(self, measurement):
        """Подготовка признаков для модели - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            # Извлекаем временные признаки
            if isinstance(measurement, list):
                # Если передали список, берем первый элемент
                measurement = measurement[0] if measurement else {}

            timestamp = datetime.fromisoformat(measurement['timestamp'].replace('Z', '+00:00'))

            # Используем правильные ключи из данных
            hvs_value = float(measurement['hvs_value'])
            gvs_value = float(measurement.get('gvs_value', measurement.get('gvs_total', 0)))
            deviation = float(measurement['deviation'])

            # Получаем значения каналов ГВС (с fallback)
            gvs_flow1 = float(measurement.get('gvs_flow1', measurement.get('gvs_flow1_value', 0)))
            gvs_flow2 = float(measurement.get('gvs_flow2', measurement.get('gvs_flow2_value', 0)))

            # Базовые признаки
            features = [
                hvs_value,  # ХВС
                gvs_value,  # ГВС общее
                deviation,  # Отклонение
                timestamp.hour,  # Час (0-23)
                timestamp.weekday(),  # День недели (0-6)
                timestamp.day,  # День месяца (1-31)
                timestamp.month,  # Месяц (1-12)
            ]

            # Дополнительные признаки
            features.extend([
                gvs_flow1,  # ГВС канал 1
                gvs_flow2,  # ГВС канал 2
                timestamp.minute,  # Минуты
                timestamp.second % 10,
                hvs_value ** 0.5,  # Квадратный корень ХВС
                gvs_value ** 0.5,  # Квадратный корень ГВС
                abs(deviation),  # Абсолютное отклонение
                deviation ** 2,  # Квадрат отклонения
            ])

            # Гарантируем 15 признаков
            if len(features) < 15:
                features.extend([0] * (15 - len(features)))
            elif len(features) > 15:
                features = features[:15]

            return np.array(features).reshape(1, -1)  # Важно: reshape для predict_proba

        except Exception as e:
            logger.error(f"❌ Ошибка подготовки признаков: {e}")
            return np.zeros((1, 15))  # Возвращаем нулевые признаки правильной формы

    def _encode_feature(self, feature_name, value):
        """Кодирование категориальных признаков"""
        if feature_name not in self.label_encoders:
            self.label_encoders[feature_name] = LabelEncoder()
            # Пока не можем обучить энкодер, вернем хэш
            return hash(str(value)) % 1000
        try:
            return self.label_encoders[feature_name].transform([str(value)])[0]
        except:
            # Если значение новое, вернем -1
            return -1

    def train_model(self):
        """Обучение модели на исторических данных - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            from models import get_db, WaterMeasurement
            db = next(get_db())

            # Получаем данные за последние 30 дней
            from datetime import datetime, timedelta
            thirty_days_ago = datetime.now() - timedelta(days=30)
            measurements = db.query(WaterMeasurement).filter(
                WaterMeasurement.measurement_timestamp >= thirty_days_ago
            ).all()

            if len(measurements) < 10:  # УМЕНЬШИЛИ ДО 10 для тестирования
                logger.warning(f"Недостаточно данных для обучения модели: {len(measurements)} записей")
                # ВОЗВРАЩАЕМ False чтобы показать что модель не обучена
                return False  # ✅ НЕ создаем rule-based модель!

            # Подготовка данных для обучения
            X = []
            y = []

            for m in measurements:
                features = self.prepare_features({
                    'hvs_value': float(m.hvs_value),
                    'gvs_value': float(m.gvs_total),
                    'gvs_flow1': float(m.gvs_flow1_value),
                    'gvs_flow2': float(m.gvs_flow2_value),
                    'deviation': float(m.deviation),
                    'timestamp': m.measurement_timestamp.isoformat(),
                    'building_id': m.building_id
                })

                if len(features) == 15:
                    X.append(features)
                    y.append(1 if float(m.deviation) > 10 else 0)

            if len(X) == 0:
                logger.warning("Не удалось подготовить данные для обучения")
                return False  # ✅ НЕ создаем rule-based модель!

            if len(np.unique(y)) < 2:
                logger.warning("Недостаточно разнообразных данных для обучения")
                return False  # ✅ НЕ создаем rule-based модель!

            # Обучение настоящей ML модели
            from sklearn.ensemble import RandomForestClassifier
            self.model = RandomForestClassifier(
                n_estimators=100,
                random_state=42,
                max_depth=10,
                min_samples_split=5
            )
            self.model.fit(X, y)

            # Сохранение модели
            self.save_model()

            accuracy = self.model.score(X, y)
            logger.info(f"✅ Настоящая ML модель обучена с точностью: {accuracy:.2f}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка обучения модели: {e}")
            return False  # ✅ НЕ создаем rule-based модель при ошибке!

    def save_model(self):
        """Сохранение модели в файл - УПРОЩЕННАЯ ВЕРСИЯ"""
        try:
            if self.model is not None:
                model_data = {
                    'model': self.model,
                    'label_encoders': self.label_encoders
                }
                joblib.dump(model_data, self.model_path)
                logger.info(f"✅ Модель сохранена в {self.model_path}")
                logger.info(f"📁 Тип модели для сохранения: {type(self.model)}")
                return True
            else:
                logger.warning("⚠️ Модель None, нечего сохранять")
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения модели: {e}")
            import traceback
            logger.error(f"❌ Детали ошибки: {traceback.format_exc()}")
            return False

    def predict_anomaly_risk(self, current_data):
        """Прогнозирование риска аномалии - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            if self.model is None or self.model == "simple_rule_based":
                # Fallback на rule-based
                deviation = current_data.get('deviation', 0)
                risk_score = min(deviation / 50.0, 1.0)
                return risk_score

            # Подготавливаем данные для предсказания
            prediction_data = current_data.copy()

            # Убедимся, что есть все необходимые поля
            if 'gvs_value' not in prediction_data and 'gvs_total' in prediction_data:
                prediction_data['gvs_value'] = prediction_data['gvs_total']

            if 'timestamp' not in prediction_data:
                prediction_data['timestamp'] = datetime.now().isoformat()

            # ИСПРАВЛЕНО: передаем словарь, а не список!
            features = self.prepare_features(prediction_data)

            # Проверяем, что модель поддерживает predict_proba
            if hasattr(self.model, 'predict_proba'):
                risk_score = self.model.predict_proba(features)[0][1]  # вероятность аномалии
            else:
                # Для rule-based моделей
                deviation = current_data.get('deviation', 0)
                risk_score = min(deviation / 50.0, 1.0)

            return risk_score

        except Exception as e:
            logger.error(f"❌ Ошибка предсказания: {e}")
            deviation = current_data.get('deviation', 0)
            return min(deviation / 50.0, 1.0)
    def get_model_info(self):
        """Информация о модели"""
        if self.model is None or self.model == "simple_rule_based":
            return {
                "model_type": "rule_based",
                "status": "active",
                "features": ["deviation_based"]
            }
        else:
            return {
                "model_type": "random_forest",
                "status": "active",
                "feature_count": len(self.model.feature_importances_),
                "feature_importance_max": float(np.max(self.model.feature_importances_)),
                "n_estimators": self.model.n_estimators
            }


# Глобальный экземпляр predictor
ml_predictor = None


def initialize_ml_predictor(database_url):
    """Инициализация ML predictor"""
    global ml_predictor
    try:
        ml_predictor = WaterConsumptionPredictor(database_url)
        logger.info("✅ ML predictor инициализирован")

        # Пытаемся обучить модель, но не блокируем запуск при ошибке
        try:
            if ml_predictor.train_model():
                model_info = ml_predictor.get_model_info()
                logger.info(f"✅ ML модель успешно обучена. Тип: {model_info['model_type']}")
            else:
                logger.warning("⚠️ ML модель не обучена, используется rule-based подход")
        except Exception as e:
            logger.error(f"❌ Ошибка при обучении модели: {e}")
            # Продолжаем работу с rule-based моделью

    except Exception as e:
        logger.error(f"❌ Ошибка инициализации ML predictor: {e}")
        ml_predictor = None


def get_ml_predictor():
    """Получить экземпляр ML predictor"""
    return ml_predictor