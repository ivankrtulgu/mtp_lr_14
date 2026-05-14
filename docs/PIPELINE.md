# Конвейер данных: Анализ банковских транзакций

Данный проект представляет собой высокопроизводительный конвейер обработки данных (ETL) для сбора, очистки и анализа банковских транзакций. В проекте реализован гибридный подход: использование **Go** для эффективного сбора данных и **Python (Polars/DuckDB)** для продвинутого аналитического анализа.

## 🏗 Архитектура

Конвейер построен по линейному принципу ETL (Extract, Transform, Load):

**Go-сборщик** $\rightarrow$ **JSONL** $\rightarrow$ **Polars** $\rightarrow$ **Parquet** $\rightarrow$ **DuckDB** $\rightarrow$ **Визуализация**

### Описание компонентов:
1.  **Go-сборщик (`/collector`)**: 
    -   **Extract (Извлечение)**: Чтение исходных данных из CSV-файла.
    -   **Transform (Преобразование)**: Использование пула воркеров (горутин) для параллельного парсинга и валидации записей.
    -   **Load (Загрузка)**: Реализация пакетной записи с таймером (Timed-Batching) для оптимизации I/O и сохранения данных в формат JSONL (каждый объект на новой строке).
2.  **Polars ETL (`/analysis/etl_pipeline.py`)**:
    -   **Интеграция**: Загрузка данных из JSONL в DataFrame Polars.
    -   **Очистка**: Удаление дубликатов, обработка пропусков (заполнение медианой/значениями по умолчанию) и приведение типов (String $\rightarrow$ Datetime).
    -   **Хранение**: Экспорт «золотого» (очищенного) датасета в формат Apache Parquet для обеспечения максимальной скорости аналитических запросов.
3.  **Анализ в DuckDB (`/analysis/sql_analysis.py`)**:
    -   Выполнение сложных SQL-агрегаций напрямую из Parquet-файла без необходимости развертывания полноценного сервера БД.
4.  **Визуализация (`/analysis/visualization.py`)**:
    -   Генерация интерактивных и статических графиков (Plotly/Matplotlib) для анализа трендов и распределения трат.

---

## 🚀 Инструкция по запуску

### 1. Настройка окружения
Убедитесь, что установлены Go и Python 3.10+.

```powershell
# Создание и активация виртуального окружения
python -m venv venv
.\venv\Scripts\activate

# Установка необходимых библиотек
pip install polars duckdb plotly matplotlib pyarrow pandas
```

### 2. Последовательность запуска
Компоненты должны запускаться в следующем порядке:

**Шаг А: Генерация синтетических данных**
```powershell
python generate_data.py
```

**Шаг Б: Сбор и конвертация данных (Go)**
```powershell
cd collector
go run main.go
cd ..
```

**Шаг В: Очистка и обработка данных (Python)**
```powershell
.\venv\Scripts\python.exe analysis/etl_pipeline.py
```

**Шаг Г: Запуск SQL-анализа и бенчмарка**
```powershell
.\venv\Scripts\python.exe analysis/sql_analysis.py
```

**Шаг Д: Генерация визуализаций**
```powershell
.\venv\Scripts\python.exe analysis/visualization.py
```

---

## 📊 Анализ и результаты

### SQL-запрос для анализа
Для анализа затрат по категориям (только для положительных сумм) использовался следующий запрос в DuckDB:

```sql
SELECT 
    category, 
    SUM(amount) as total_amount, 
    AVG(amount) as avg_amount, 
    MIN(amount) as min_amount, 
    MAX(amount) as max_amount, 
    COUNT(*) as transaction_count
FROM 'data/processed/transactions.parquet'
WHERE amount > 0
GROUP BY category
ORDER BY total_amount DESC
```

### Сравнение производительности
| Инструмент | Время выполнения (мс) | Примечание |
|---|---|---|
| **Polars** | ~75 мс | Максимальная скорость для данных в памяти |
| **DuckDB** | ~3680 мс | Накладные расходы на SQL-движок при малых объемах |

### Созданные визуализации
-   `plots/category_distribution.html`: Интерактивная круговая диаграмма распределения общей суммы трат по категориям.
-   `plots/spending_trend.png`: Линейный график ежедневного объема транзакций.

---

## 📁 Структура проекта
- `data/raw/` : Исходные CSV-файлы.
- `data/intermediate/` : Промежуточный вывод Go-сборщика (JSONL).
- `data/processed/` : Итоговые Parquet-файлы.
- `collector/` : Исходный код на Go.
- `analysis/` : Python-скрипты для ETL, SQL и визуализации.
- `plots/` : Результаты визуализации.
- `PROMPT_LOG.md` : Полный журнал промптов и процесса разработки.
