# Лабораторная работа 2  
## Формирование отчётов в Apache Spark

## Цель работы

Сформировать отчёт о 10 наиболее популярных языках программирования по годам за период с 2010 по 2020 годы на основе данных StackOverflow.

Итоговый отчёт сохраняется в формате **Apache Parquet**.

---

## Используемые данные

В работе использовались следующие файлы:

- `posts_sample.xml` — тестовая выборка постов StackOverflow;
- `programming-languages.csv` — список языков программирования;
- `report.scala` — Spark-скрипт для формирования отчёта.

---

## Используемое окружение

Работа выполнялась в Docker-контейнере со Spark:

- Docker + WSL2 Ubuntu;
- образ: `apache/spark:latest`;
- Spark version: `4.1.1`;
- Scala version: `2.13.17`;
- режим запуска Spark: `local[*]`.

---

## Команда запуска контейнера

```bash
docker run -it --rm \
  --name spark-lab2 \
  -v "$PWD":/work \
  apache/spark:latest bash
