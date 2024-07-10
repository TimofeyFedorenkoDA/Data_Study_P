# Проект по Анализу Продаж сети магазинов Superstore

## Описание проекта

Проект направлен на проведение комплексного анализа данных о продажах с использованием различных инструментов. Первоначально данные проходят первичную обработку на предмет чистоты, затем проводится анализ динамики продаж и выявление трендов по городам и категориям товаров. Итоговый датафрейм выгружается в базу данных, после чего проводится RFM-анализ для сегментации клиентов. Завершающим этапом является создание интерактивного дашборда с использованием BI системы Datalens для наглядной визуализации полученных данных.

## Цели проекта

### Средствами Python

1. **Первичный анализ данных**:
    - Избавиться от дубликатов и выбросов.
    - По необходимости добавить расчетные столбцы.

2. **Анализ динамики продаж**:
    - Проследить динамику продаж за весь период.
    - Сделать разбивку по топ-5 городам.
    - Рассмотреть популярные категории товаров и выявить тренды.

3. **Выгрузка данных в базу данных**:
    - Выгрузить итоговый датафрейм в базу данных (PostgreSQL или SQLite).

### Средствами SQL

1. **RFM-анализ**:
    - Провести RFM-анализ по итоговому датафрейму для двух последних лет.
    - Разбить клиентов на 4 сегмента: Лояльные клиенты, Новые клиенты, Спящие клиенты, Потерянные клиенты.

### Средствами BI системы Datalens

1. **Построение интерактивного дашборда**:
    - Создать аналитический дашборд для визуализации данных и сравнить финансовые показатели сегментированных клиентов за 2 года

## Структура файлов проекта

- **project_ver2.ipynb**: Тетрадка с кодом проекта.
- **superstore.csv**: Датасет, который подгружается из Google Drive в тетрадке.
- **superstore_Sqlite.db**: База данных SQLite с готовыми расчетами и таблицами.
- **RFM Анализ в SQL и комментарии.sql**: Код SQL для проведения RFM-анализа.
- **df_rfm_2017_202404191644.csv**: Датафрейм со всеми данными за 2017 год и RFM метками.
- **df_rfm_2018_202404191644.csv**: Датафрейм со всеми данными за 2018 год и RFM метками.

## Использованные инструменты и технологии

- **Язык программирования**: Python
- **Библиотеки**: pandas, numpy, matplotlib, seaborn, sqlalchemy, sqlite3
- **СУБД**: DBeaver для осуществления запросов к базе
- **SQL**: Для проведения RFM-анализа и работы с базой данных
- **База данных**: SQLite
- **BI система**: Datalens


## Инструкции по использованию

1. Скачайте файлы.
2. Откройте и запустите `project_ver2.ipynb` для выполнения анализа и подготовки данных.
3. Подключите базу данных SQLite или PostgreSQL для выгрузки итогового датафрейма.
4. Используйте `RFM Анализ в SQL и комментарии.sql` для проведения RFM-анализа самостоятельно или используйте готовый файл.
5. Загрузите данные в BI систему Datalens для создания интерактивного дашборда.



