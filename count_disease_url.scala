// Execute: dse spark -i:count_disease_url.scala
import org.apache.spark.sql.functions._

//-------------------------------------------------------------------------------------------------------------------------//
val disease = "health"
csc.setKeyspace("disease")
csc.sql("CONSISTENCY ANY")

//-------------------------------------------------------------------------------------------------------------------------//
// load engine visisted and filter disease within url
val df1 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "visited" )).load
val diseaseDF = df1.filter(col("url").contains(disease))
// diseaseDF.take(10).foreach(println)

//-------------------------------------------------------------------------------------------------------------------------//
// create table
// val table = csc.sql("CREATE TABLE IF NOT EXISTS " + disease + " ( url text PRIMARY KEY);")

//-------------------------------------------------------------------------------------------------------------------------//
// Save output to disease table
diseaseDF.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> disease )).save

//-------------------------------------------------------------------------------------------------------------------------//
// Count newly found records
val df2 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> disease )).load
df2.count()

