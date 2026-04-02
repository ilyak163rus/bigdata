
import org.apache.log4j.{Level, Logger}
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)


val stationsPath = "stations.csv"
val outputPath = "/home/mapr/bike_lab_result_submit.txt"

case class Station(
  stationId: Int,
  name: String,
  lat: Double,
  lon: Double,
  dockCount: Int,
  city: String,
  installationDate: String
)

case class Trip(
  tripId: Int,
  duration: Int,
  startDate: LocalDateTime,
  startStation: String,
  startTerminal: Int,
  endDate: LocalDateTime,
  endStation: String,
  endTerminal: Int,
  bikeId: Int,
  subscriptionType: String,
  zipCode: String
)

def formatDuration(totalSeconds: Long): String = {
  val hours = totalSeconds / 3600
  val minutes = (totalSeconds % 3600) / 60
  val seconds = totalSeconds % 60
  f"$hours%02d:$minutes%02d:$seconds%02d"
}

def haversineKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
  val earthRadiusKm = 6371.0088
  val dLat = math.toRadians(lat2 - lat1)
  val dLon = math.toRadians(lon2 - lon1)
  val a =
    math.pow(math.sin(dLat / 2), 2) +
      math.cos(math.toRadians(lat1)) *
      math.cos(math.toRadians(lat2)) *
      math.pow(math.sin(dLon / 2), 2)
  val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
  earthRadiusKm * c
}

def validZip(zip: String): Boolean = {
  zip != null && zip.trim.nonEmpty && zip.trim.toLowerCase != "nil"
}

val tripsRaw = sc.textFile(tripsPath)
val stationsRaw = sc.textFile(stationsPath)

val tripsHeader = tripsRaw.first()
val stationsHeader = stationsRaw.first()

val trips = tripsRaw.filter(_ != tripsHeader).map(_.split(",", -1))
val stations = stationsRaw.filter(_ != stationsHeader).map(_.split(",", -1))

val tripsInternal = trips.mapPartitions { rows =>
  val timeFormat = DateTimeFormatter.ofPattern("M/d/yyyy H:m")
  rows.map { row =>
    Trip(
      tripId = row(0).trim.toInt,
      duration = row(1).trim.toInt,
      startDate = LocalDateTime.parse(row(2).trim, timeFormat),
      startStation = row(3).trim,
      startTerminal = row(4).trim.toInt,
      endDate = LocalDateTime.parse(row(5).trim, timeFormat),
      endStation = row(6).trim,
      endTerminal = row(7).trim.toInt,
      bikeId = row(8).trim.toInt,
      subscriptionType = row(9).trim,
      zipCode = row(10).trim
    )
  }
}.cache()

val stationsInternal = stations.map { row =>
  Station(
    stationId = row(0).trim.toInt,
    name = row(1).trim,
    lat = row(2).trim.toDouble,
    lon = row(3).trim.toDouble,
    dockCount = row(4).trim.toInt,
    city = row(5).trim,
    installationDate = row(6).trim
  )
}.cache()

val tripsCount = tripsInternal.count()
val stationsCount = stationsInternal.count()

val maxBike = tripsInternal
  .map(trip => (trip.bikeId, trip.duration.toLong))
  .reduceByKey(_ + _)
  .max()(Ordering.by[(Int, Long), Long](_._2))

val maxBikeId = maxBike._1
val maxBikeTotalDuration = maxBike._2

val maxDistance = stationsInternal
  .cartesian(stationsInternal)
  .filter { case (left, right) => left.stationId < right.stationId }
  .map { case (left, right) =>
    (haversineKm(left.lat, left.lon, right.lat, right.lon), left, right)
  }
  .max()(Ordering.by[(Double, Station, Station), Double](_._1))

val maxBikeTrips = tripsInternal
  .filter(_.bikeId == maxBikeId)
  .sortBy(trip => (trip.startDate.toEpochSecond(ZoneOffset.UTC), trip.tripId), ascending = true)
  .collect()

val bikePath =
  if (maxBikeTrips.isEmpty) "path not found"
  else (Seq(maxBikeTrips.head.startStation) ++ maxBikeTrips.map(_.endStation)).mkString(" -> ")

val bikesCount = tripsInternal.map(_.bikeId).distinct().count()

val heavyUsers = tripsInternal
  .filter(trip => validZip(trip.zipCode))
  .map(trip => (trip.zipCode.trim, trip.duration.toLong))
  .reduceByKey(_ + _)
  .filter { case (_, totalSec) => totalSec > 3L * 60L * 60L }
  .sortBy({ case (_, totalSec) => -totalSec })
  .collect()

val resultLines =
  Seq(
    "=" * 90,
    "ЛАБОРАТОРНАЯ РАБОТА 1: анализ данных велопарковок San Francisco Bay Area Bike Share",
    "=" * 90,
    s"Путь к trips: $tripsPath",
    s"Путь к stations: $stationsPath",
    s"Количество поездок: $tripsCount",
    s"Количество станций: $stationsCount",
    "",
    "1. Велосипед с максимальным временем пробега",
    s"bikeId = $maxBikeId",
    s"Суммарное время = ${formatDuration(maxBikeTotalDuration)} ($maxBikeTotalDuration сек.)",
    "",
    "2. Наибольшее геодезическое расстояние между станциями",
    s"Станция 1: ${maxDistance._2.name} (#${maxDistance._2.stationId})",
    s"Станция 2: ${maxDistance._3.name} (#${maxDistance._3.stationId})",
    f"Расстояние = ${maxDistance._1}%.3f км",
    "",
    "3. Путь велосипеда с максимальным временем пробега через станции",
    s"bikeId = $maxBikeId",
    s"Количество поездок этого велосипеда = ${maxBikeTrips.length}",
    s"Путь = $bikePath",
    "",
    "4. Количество велосипедов в системе",
    s"Количество велосипедов = $bikesCount",
    "",
    "5. Пользователи, потратившие на поездки более 3 часов",
    s"Количество найденных пользователей = ${heavyUsers.length}",
    "Примечание: отдельного userId в trips.csv нет, поэтому в качестве доступного идентификатора использован zipCode; пропуски и значение nil исключены.",
    ""
  ) ++
    heavyUsers.map { case (zip, totalSec) =>
      s"zipCode = $zip, суммарное время = ${formatDuration(totalSec)} ($totalSec сек.)"
    } ++
    Seq(
      "",
      "Детализация поездок велосипеда с максимальным временем пробега:"
    ) ++
    maxBikeTrips.map { trip =>
      s"${trip.startDate} | ${trip.startStation} (#${trip.startTerminal}) -> ${trip.endStation} (#${trip.endTerminal}) | ${trip.duration} сек."
    } ++
    Seq("=" * 90)

resultLines.foreach(println)
Files.write(Paths.get(outputPath), resultLines.asJava, StandardCharsets.UTF_8)
println(s"\nРезультат также сохранён в файл: $outputPath")
