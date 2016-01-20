// Execute: dse spark -i:count_disease_url.scala
import org.apache.spark.sql.functions._

val disease = "hepatitis"

csc.setKeyspace("disease")

// load engine visisted
val df1 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "visited" )).load

// filter disease within url
val diseaseDF = df1.filter(col("url").contains(disease))

// output sample on the screen
// diseaseDF.take(10).foreach(println)

// create table
val table = csc.sql("CREATE TABLE IF NOT EXISTS " + disease + " ( url text PRIMARY KEY);")

// Save output to disease table
diseaseDF.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> disease )).save

val df2 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> disease )).load
df2.count()

