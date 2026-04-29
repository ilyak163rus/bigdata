# Лабораторная работа 3
## Потоковая обработка в Apache Flink

Выполнены упражнения из репозитория flink-training-exercises:

1. RideCleansingExercise
2. RidesAndFaresExercise
3. HourlyTipsExercise
4. ExpiringStateExercise

## Используемое окружение

- ОС: Windows + WSL2 Ubuntu
- Docker Desktop с WSL Integration
- Контейнер для сборки: maven:3.8.8-eclipse-temurin-8
- Язык решения: Scala
- Данные:
  - nycTaxiRides.gz
  - nycTaxiFares.gz

## Изменённые файлы

- src/main/scala/com/ververica/flinktraining/exercises/datastream_scala/basics/RideCleansingExercise.scala
- src/main/scala/com/ververica/flinktraining/exercises/datastream_scala/state/RidesAndFaresExercise.scala
- src/main/scala/com/ververica/flinktraining/exercises/datastream_scala/windows/HourlyTipsExercise.scala
- src/main/scala/com/ververica/flinktraining/exercises/datastream_scala/process/ExpiringStateExercise.scala
- src/main/java/com/ververica/flinktraining/exercises/datastream_java/utils/ExerciseBase.java
- pom.xml

## Описание решений

### RideCleansingExercise

Реализована фильтрация потока поездок. В результирующий поток попадают только поездки, у которых начальная и конечная точки находятся в пределах Нью-Йорка. Для проверки координат используется GeoUtils.isInNYC.

### RidesAndFaresExercise

Реализовано соединение двух потоков: TaxiRide и TaxiFare. Потоки соединяются по rideId. Если одна часть события пришла раньше другой, она временно сохраняется в ValueState. После получения пары результат выводится как кортеж TaxiRide и TaxiFare.

### HourlyTipsExercise

Реализован расчёт чаевых по водителям за каждый час. Сначала для каждого водителя считается сумма чаевых в часовом окне, затем среди всех водителей выбирается максимальная сумма чаевых за этот час.

### ExpiringStateExercise

Реализовано соединение TaxiRide и TaxiFare с использованием KeyedCoProcessFunction, ValueState и event-time таймеров. Если соответствующая пара не находится за заданное время, запись выводится в side output как unmatched.

## Проверка

Тесты запускались командой:

mvn -Dcheckstyle.skip=true -Dtest="RideCleansingScalaTest,RidesAndFaresScalaTest,HourlyTipsScalaTest,ExpiringStateScalaTest" test

Результат проверки сохранён в файле:

lab3-test-results.txt

Все тесты завершились успешно.
