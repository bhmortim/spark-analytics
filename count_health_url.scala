import org.apache.spark.sql.functions._

val df = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "visited" )).load

val healthDF = df.filter(col("url").contains("health"))

healthDF.take(100).foreach(println)

