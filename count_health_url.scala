import org.apache.spark.sql.functions._

// load engine visisted
val df = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "visited" )).load

// filter health within url
val healthDF = df.filter(col("url").contains("health"))

// output sample on the screen
healthDF.take(10).foreach(println)

// Save output to health table
healthDF.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "health" )).save


