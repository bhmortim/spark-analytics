//-------------------------------------------------------------------------------------------------------------------------//
// Execute: Spark Job 
// dse spark -i:count_disease_url.scala
import org.apache.spark.sql.functions._

val diseases=Array("mental","headache","cardio","disease","heart","fungal","gerson","vulgaris")

//-------------------------------------------------------------------------------------------------------------------------//
csc.setKeyspace("disease")

//-------------------------------------------------------------------------------------------------------------------------//
val df1 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "visited" )).load

diseases.foreach( disease => {
    println(disease)
    val df2 = df1.filter(col("url").contains(disease))
    val df3 = diseaseDF.withColumn("condition", disease ) 
    df3.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> "general" )).save
} )

//-------------------------------------------------------------------------------------------------------------------------//
// Count records
val df4 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> "general" )).load
df4.count()
//-------------------------------------------------------------------------------------------------------------------------//

