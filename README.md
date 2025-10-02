# Mosvodokanal Project

Система мониторинга водопотребления с ML-обнаружением аномалий.

## Запуск
```bash
docker-compose up -d

Открыть: http://localhost:5000
Логин: admin
Пароль: password

## 📡 API Documentation

### Основные эндпоинты

#### 🏢 Данные зданий
| Метод | URL | Описание |
|-------|-----|-----------|
| GET | `/api/buildings` | Список всех зданий |
| GET | `/api/measurements/{building_id}` | Данные конкретного здания |

#### 📊 Прогнозы и аналитика
| Метод | URL | Описание |
|-------|-----|-----------|
| GET | `/api/predictions` | Прогнозы рисков для всех зданий |
| GET | `/api/risk_summary` | Сводка по уровням риска |
| GET | `/api/stats` | Общая статистика системы |

#### 📈 Графики
| Метод | URL | Параметры | Описание |
|-------|-----|-----------|-----------|
| GET | `/api/charts/consumption` | `period=30&building_id=all` | Данные потребления |
| GET | `/api/charts/trend` | `period=30&building_id=all` | Тренды потребления |
| GET | `/api/charts/risks` | - | Топ рисковых зданий |

#### 🤖 ML Модель
| Метод | URL | Описание |
|-------|-----|-----------|
| GET | `/api/model_status` | Статус модели |
| POST | `/api/train_model` | Переобучение модели |
| GET | `/api/debug/ml_status` | Детальная диагностика |

#### ⚙️ Системные
| Метод | URL | Описание |
|-------|-----|-----------|
| GET | `/health` | Health check |
| GET | `/api/itp` | Список ИТП |
| GET | `/api/mkd` | Список МКД |

### Примеры использования

#### Получить все прогнозы:
```bash
curl http://localhost:5000/api/predictions
