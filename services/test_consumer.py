import json
import logging
import os
from datetime import datetime
from kafka import KafkaConsumer
import time
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestConsumer:
    def __init__(self, bootstrap_servers=None, topics=None):
        # ИСПРАВЛЕНИЕ: используем kafka:9092 по умолчанию для Docker
        self.bootstrap_servers = bootstrap_servers or ['kafka:9092']
        self.topics = topics or ['water_measurements', 'raw-measurements']  # ОБНОВЛЕНО: правильное имя топика
        self.running = True

    def calculate_anomaly(self, data):
        """Расчет аномалии между ХВС и ГВС с поддержкой двух каналов"""
        try:
            # ОБНОВЛЕНО: Поддержка новой структуры с двумя каналами ГВС
            cold_water = data.get('cold_water', data.get('hvs_value', 0))

            # ОБНОВЛЕНО: Используем gvs_total если есть, иначе вычисляем из двух каналов
            if 'gvs_total' in data:
                hot_water = data.get('gvs_total')
            elif 'gvs_flow1_value' in data and 'gvs_flow2_value' in data:
                # Расчет для циркуляционной схемы
                hot_water = float(data.get('gvs_flow1_value', 0)) - float(data.get('gvs_flow2_value', 0))
            else:
                # Старая структура (для обратной совместимости)
                hot_water = data.get('hot_water', data.get('gvs_value', 0))

            cold_water = float(cold_water) if cold_water else 0
            hot_water = float(hot_water) if hot_water else 0

            if cold_water > 0:
                deviation = abs((hot_water / cold_water) - 1) * 100
            else:
                deviation = 0

            is_anomaly = deviation > 10
            return deviation, is_anomaly

        except Exception as e:
            logger.error(f"Ошибка расчета аномалии: {e}")
            return 0, False

    def print_message_info(self, message, topic):
        """Выводит информацию о сообщении с поддержкой новой структуры"""
        try:
            data = message.value
            deviation, is_anomaly = self.calculate_anomaly(data)

            print("=" * 80)
            print(f"📨 ТОПИК: {topic}")
            print(f"⏰ ВРЕМЯ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"🔑 КЛЮЧ: {message.key}")
            print(f"📊 ПАРТИЦИЯ: {message.partition}")
            print(f"📍 ОФФСЕТ: {message.offset}")
            print("-" * 40)

            # Основная информация
            address = data.get('address', data.get('building', 'Неизвестный адрес'))
            print(f"🏢 АДРЕС: {address}")
            print(f"🆔 ID здания: {data.get('building_id', 'N/A')}")
            print(f"🏭 ID ИТП: {data.get('itp_id', 'N/A')}")  # ОБНОВЛЕНО: Добавлен ИТП
            print(f"📟 ID устройства: {data.get('device_id', 'N/A')}")
            print(f"🎯 СЦЕНАРИЙ: {data.get('scenario', 'N/A')}")

            # ОБНОВЛЕНО: Показания с поддержкой двух каналов ГВС
            cold_water = data.get('cold_water', data.get('hvs_value', 'N/A'))

            # Отображаем оба канала ГВС если есть
            if 'gvs_flow1_value' in data and 'gvs_flow2_value' in data:
                gvs_flow1 = data.get('gvs_flow1_value', 'N/A')
                gvs_flow2 = data.get('gvs_flow2_value', 'N/A')
                gvs_total = data.get('gvs_total', 'N/A')
                print(f"💧 ХВС: {cold_water}")
                print(f"🔥 ГВС канал 1: {gvs_flow1}")
                print(f"🔥 ГВС канал 2: {gvs_flow2}")
                print(f"🔥 ГВС ОБЩЕЕ: {gvs_total}")
            else:
                # Старая структура
                hot_water = data.get('hot_water', data.get('gvs_value', 'N/A'))
                print(f"💧 ХВС: {cold_water}")
                print(f"🔥 ГВС: {hot_water}")

            print(f"📈 ОТКЛОНЕНИЕ: {deviation:.2f}%")

            # Статус аномалии
            if is_anomaly:
                print(f"🚨 СТАТУС: АНОМАЛИЯ (>{10}%)")
            else:
                print(f"✅ СТАТУС: НОРМА")

            # ОБНОВЛЕНО: Дополнительные поля новой структуры
            if 'hvs_water_meter_id' in data:
                print(f"📊 Счетчик ХВС: {data.get('hvs_water_meter_id')}")
            if 'gvs_flowmeter1_id' in data:
                print(f"📊 Счетчик ГВС1: {data.get('gvs_flowmeter1_id')}")
            if 'gvs_flowmeter2_id' in data:
                print(f"📊 Счетчик ГВС2: {data.get('gvs_flowmeter2_id')}")
            if 'measurement_id' in data:
                print(f"📨 ID измерения: {data.get('measurement_id')}")

            # Существующие поля
            if 'fias_id' in data:
                print(f"🏷️ FIAS ID: {data.get('fias_id')}")
            if 'unom' in data:
                print(f"🏷️ УНОМ: {data.get('unom')}")
            if 'message_id' in data:
                print(f"📨 ID сообщения: {data.get('message_id')}")

            print(f"🕒 TIMESTAMP: {data.get('timestamp', 'N/A')}")
            print("=" * 80)
            print()

        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}")
            print(f"❌ Ошибка: {e}")
            print(f"📦 Сырые данные: {message.value}")
            print()

    def start_consuming(self):
        """Запускает потребитель для тестирования"""
        try:
            consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='test-consumer-group',
                enable_auto_commit=False
            )

            logger.info(f"🚀 Test Consumer запущен")
            logger.info(f"📡 Подключение к: {self.bootstrap_servers}")
            logger.info(f"🎯 Мониторинг топиков: {', '.join(self.topics)}")
            logger.info("⏹️  Для остановки нажмите Ctrl+C")
            print()

            message_count = 0
            start_time = time.time()

            for message in consumer:
                if not self.running:
                    break

                message_count += 1
                self.print_message_info(message, message.topic)

                # Статистика каждые 10 сообщений
                if message_count % 10 == 0:
                    elapsed_time = time.time() - start_time
                    print(f"📊 СТАТИСТИКА: Обработано {message_count} сообщений за {elapsed_time:.1f} сек")
                    print()

        except KeyboardInterrupt:
            logger.info("⏹️  Остановка по запросу пользователя")
        except Exception as e:
            logger.error(f"❌ Ошибка в consumer: {e}")
        finally:
            if 'consumer' in locals():
                consumer.close()

    def stop(self):
        self.running = False


def simple_consumer():
    """Простой потребитель для быстрой проверки данных"""
    try:
        # ИСПРАВЛЕНИЕ: используем kafka:9092 по умолчанию
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        consumer = KafkaConsumer(
            'water_measurements',  # ОБНОВЛЕНО: правильное имя топика
            bootstrap_servers=[bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='simple-test-group'
        )

        print("🚀 Simple Test Consumer запущен")
        print(f"📡 Подключение к: {bootstrap_servers}")
        print("📝 Ожидание сообщений... (Ctrl+C для остановки)")
        print()

        for i, message in enumerate(consumer):
            data = message.value
            address = data.get('address', data.get('building', 'Unknown'))

            print(f"📨 Сообщение #{i + 1}")
            print(f"🏢 {address}")
            print(f"💧 ХВС: {data.get('hvs_value', data.get('cold_water', 'N/A'))}")

            # ОБНОВЛЕНО: Поддержка двух каналов ГВС
            if 'gvs_flow1_value' in data:
                print(f"🔥 ГВС1: {data.get('gvs_flow1_value', 'N/A')}")
                print(f"🔥 ГВС2: {data.get('gvs_flow2_value', 'N/A')}")
                print(f"🔥 ГВС ОБЩ: {data.get('gvs_total', 'N/A')}")
            else:
                print(f"🔥 ГВС: {data.get('gvs_value', data.get('hot_water', 'N/A'))}")

            print(f"🎯 {data.get('scenario', 'N/A')}")
            print(f"🏭 ИТП: {data.get('itp_id', 'N/A')}")
            print("-" * 50)

    except KeyboardInterrupt:
        print("\n👋 Завершение работы...")
    except Exception as e:
        print(f"💥 Ошибка: {e}")


def main():
    """Основная функция для тестирования конкретного топика"""
    import argparse

    parser = argparse.ArgumentParser(description='Test Kafka Consumer для Мосводоканал')
    parser.add_argument('--topic', type=str, default='water_measurements',  # ОБНОВЛЕНО: правильное имя по умолчанию
                        help='Топик для мониторинга (по умолчанию: water_measurements)')
    parser.add_argument('--bootstrap-servers', type=str, default='kafka:9092',
                        help='Kafka bootstrap servers (по умолчанию: kafka:9092)')
    parser.add_argument('--from-beginning', action='store_true',
                        help='Читать сообщения с начала')
    parser.add_argument('--simple', action='store_true',
                        help='Запустить простую версию consumer')

    args = parser.parse_args()

    if args.simple:
        simple_consumer()
        return

    consumer = TestConsumer()
    consumer.bootstrap_servers = [args.bootstrap_servers]
    consumer.topics = [args.topic]

    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        print("\n👋 Завершение работы...")
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")


if __name__ == "__main__":
    main()