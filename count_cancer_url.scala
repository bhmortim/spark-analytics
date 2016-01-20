import org.apache.spark.sql.functions._

// load engine visisted
val df = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "visited" )).load

// filter cancer within url
val cancerDF = df.filter(col("url").contains("cancer"))

// output sample on the screen
cancerDF.take(10).foreach(println)

// Save output to cancer table
cancerDF.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "cancer" )).save

val df2 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "cancer" )).load
df2.count()

