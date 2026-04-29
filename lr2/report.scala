import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import spark.implicits._

val postsPath = sys.env.getOrElse("POSTS", "/work/posts_sample.xml")
val languagesPath = sys.env.getOrElse("LANGS", "/work/programming-languages.csv")
val outBase = sys.env.getOrElse("OUT_BASE", "/tmp/out_sample")

val postsParquetPath = s"$outBase/posts_parquet"
val reportParquetPath = s"$outBase/report_parquet"

println(s"Posts XML: $postsPath")
println(s"Languages CSV: $languagesPath")
println(s"Output base: $outBase")

val rawPosts = spark.read
  .text(postsPath)
  .select($"value".as("line"))
  .where(trim($"line").startsWith("<row"))

val postTags = rawPosts
  .select(
    regexp_extract($"line", """PostTypeId="([^"]*)"""", 1).as("post_type_id"),
    regexp_extract($"line", """CreationDate="([0-9]{4})""", 1).as("year_str"),
    regexp_extract($"line", """Tags="([^"]*)"""", 1).as("tags_raw")
  )
  .where($"post_type_id" === "1")
  .where($"year_str" =!= "")
  .where($"tags_raw" =!= "")
  .withColumn("year", $"year_str".cast("int"))
  .where($"year".between(2010, 2020))
  .withColumn(
    "tags_line",
    regexp_replace(
      regexp_replace($"tags_raw", "&lt;", ""),
      "&gt;",
      ","
    )
  )
  .withColumn("tag", explode(split($"tags_line", ",")))
  .withColumn("tag", lower(trim($"tag")))
  .where($"tag" =!= "")
  .select($"year", $"tag")

postTags.write
  .mode("overwrite")
  .parquet(postsParquetPath)

println(s"Posts parquet saved to: $postsParquetPath")

val languagesFromCsv = spark.read
  .option("header", "true")
  .option("inferSchema", "false")
  .csv(languagesPath)
  .select(trim($"name").as("language"))
  .where($"language".isNotNull)
  .where($"language" =!= "")
  .withColumn("tag", lower($"language"))
  .withColumn("tag", regexp_replace($"tag", """\s*\(.*?\)\s*""", ""))
  .withColumn("tag", regexp_replace($"tag", """\s+""", "-"))
  .withColumn("tag", regexp_replace($"tag", """-+""", "-"))
  .withColumn("tag", regexp_replace($"tag", """^-|-$""", ""))
  .select($"language", $"tag")
  .where($"tag" =!= "")

val manualAliases = Seq(
  ("C", "c"),
  ("C++", "c++"),
  ("C#", "c#"),
  ("F#", "f#"),
  ("R", "r"),
  ("Go", "go"),
  ("Java", "java"),
  ("JavaScript", "javascript"),
  ("TypeScript", "typescript"),
  ("Python", "python"),
  ("PHP", "php"),
  ("Ruby", "ruby"),
  ("Scala", "scala"),
  ("Kotlin", "kotlin"),
  ("Swift", "swift"),
  ("Rust", "rust"),
  ("Dart", "dart"),
  ("SQL", "sql"),
  ("Perl", "perl"),
  ("Lua", "lua"),
  ("MATLAB", "matlab"),
  ("Objective-C", "objective-c"),
  ("Visual Basic .NET", "vb.net"),
  ("Visual Basic", "visual-basic"),
  ("Assembly", "assembly"),
  ("Shell", "shell"),
  ("Bash", "bash")
).toDF("language", "tag")

val languages = manualAliases
  .unionByName(languagesFromCsv)
  .dropDuplicates("tag")

val posts = spark.read.parquet(postsParquetPath)

val counts = posts
  .join(broadcast(languages), Seq("tag"), "inner")
  .groupBy($"year", $"language")
  .count()

val windowByYear = Window
  .partitionBy($"year")
  .orderBy($"count".desc, $"language".asc)

val report = counts
  .withColumn("rank", row_number().over(windowByYear))
  .where($"rank" <= 10)
  .select($"year", $"rank", $"language", $"count")
  .orderBy($"year", $"rank")

report.write
  .mode("overwrite")
  .parquet(reportParquetPath)

println(s"Final report saved to: $reportParquetPath")

report.show(120, false)

println("Done.")
