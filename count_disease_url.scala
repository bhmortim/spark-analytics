// Execute: dse spark -i:count_disease_url.scala
import org.apache.spark.sql.functions._

val disease = "gastrointestinal"

// load engine visisted
val df = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "visited" )).load

// filter disease within url
val diseaseDF = df.filter(col("url").contains(disease))

// output sample on the screen
// diseaseDF.take(10).foreach(println)

// Save output to disease table
diseaseDF.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> disease )).save

val df2 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> disease )).load
df2.count()

