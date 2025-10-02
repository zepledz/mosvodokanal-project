-- init.sql
-- Удаляем старые таблицы если существуют
DROP TABLE IF EXISTS ml_features;
DROP TABLE IF EXISTS building_stats;
DROP TABLE IF EXISTS water_measurements;

-- НОВАЯ СТРУКТУРА: Таблица ИТП
CREATE TABLE IF NOT EXISTS itp (
    itp_id VARCHAR(50) PRIMARY KEY,
    itp_number VARCHAR(100) NOT NULL,
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- НОВАЯ СТРУКТУРА: Таблица МКД с привязкой к ИТП
CREATE TABLE IF NOT EXISTS mkd (
    building_id VARCHAR(50) PRIMARY KEY,
    address TEXT NOT NULL,
    fias_id VARCHAR(50),
    unom VARCHAR(50),
    itp_id VARCHAR(50) REFERENCES itp(itp_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ОБНОВЛЕННАЯ СТРУКТУРА: Измерения с двумя каналами ГВС
CREATE TABLE IF NOT EXISTS water_measurements (
    id SERIAL PRIMARY KEY,
    measurement_id VARCHAR(50) NOT NULL,
    device_id VARCHAR(100) NOT NULL,
    building_id VARCHAR(50) NOT NULL,
    address TEXT NOT NULL,
    fias_id VARCHAR(50),
    unom VARCHAR(50),

    -- ХВС данные
    hvs_value DECIMAL(10,3) NOT NULL,
    hvs_water_meter_id VARCHAR(100),

    -- ГВС данные (ДВА КАНАЛА)
    gvs_flow1_value DECIMAL(10,3) NOT NULL,
    gvs_flow2_value DECIMAL(10,3) DEFAULT 0,
    gvs_total DECIMAL(10,3),
    gvs_flowmeter1_id VARCHAR(100),
    gvs_flowmeter2_id VARCHAR(100),

    -- Расчетные поля
    deviation DECIMAL(5,2) DEFAULT 0,
    is_anomaly BOOLEAN DEFAULT FALSE,
    scenario VARCHAR(20) NOT NULL,

    -- ИТП информация
    itp_id VARCHAR(50) REFERENCES itp(itp_id),

    -- Временные метки
    measurement_timestamp TIMESTAMP NOT NULL,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ОБНОВЛЕННАЯ СТРУКТУРА: Статистика зданий с ИТП
CREATE TABLE IF NOT EXISTS building_stats (
    building_id VARCHAR(50) PRIMARY KEY,
    address TEXT NOT NULL,

    -- ИТП информация (НОВОЕ ПОЛЕ)
    itp_id VARCHAR(50) REFERENCES itp(itp_id),

    -- Статистика
    total_measurements INTEGER DEFAULT 0,
    anomaly_count INTEGER DEFAULT 0,
    avg_hvs_value DECIMAL(10,3),
    avg_gvs_value DECIMAL(10,3),
    last_measurement TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для производительности
CREATE INDEX IF NOT EXISTS idx_water_building_id ON water_measurements(building_id);
CREATE INDEX IF NOT EXISTS idx_water_timestamp ON water_measurements(measurement_timestamp);
CREATE INDEX IF NOT EXISTS idx_water_anomaly ON water_measurements(is_anomaly);
CREATE INDEX IF NOT EXISTS idx_water_device_id ON water_measurements(device_id);
CREATE INDEX IF NOT EXISTS idx_water_itp_id ON water_measurements(itp_id);

CREATE INDEX IF NOT EXISTS idx_stats_itp_id ON building_stats(itp_id);
CREATE INDEX IF NOT EXISTS idx_stats_last_measurement ON building_stats(last_measurement);

-- Таблица для ML features
CREATE TABLE IF NOT EXISTS ml_features (
    id SERIAL PRIMARY KEY,
    building_id VARCHAR(50) NOT NULL,
    feature_date DATE NOT NULL,
    hvs_mean DECIMAL(10,3),
    gvs_mean DECIMAL(10,3),
    hvs_std DECIMAL(10,3),
    gvs_std DECIMAL(10,3),
    anomaly_score DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(building_id, feature_date)
);

-- Тестовые данные ИТП
INSERT INTO itp (itp_id, itp_number, address) VALUES
('itp_1', 'ИТП-001', 'г. Москва, ЦАО, ул. Тверская, д. 1'),
('itp_2', 'ИТП-002', 'г. Москва, САО, Ленинградский пр-т, д. 25'),
('itp_3', 'ИТП-003', 'г. Москва, ЮАО, ул. Профсоюзная, д. 10'),
('itp_4', 'ИТП-004', 'г. Москва, ВАО, Щелковское шоссе, д. 75'),
('itp_5', 'ИТП-005', 'г. Москва, ЗАО, ул. Минская, д. 15')
ON CONFLICT (itp_id) DO NOTHING;