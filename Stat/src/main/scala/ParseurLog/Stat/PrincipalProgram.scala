package ParseurLog.Stat

import org.apache.spark.sql.Encoders
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.split 
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object PrincipalProgram extends App{

  
  
  import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .master("local[*]")
  .appName("Spark SQL parseur de log")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

import spark.implicits._
import spark.sql

case class LogSchema(Time: String, url: String)
val logSchema = Encoders.product[LogSchema].schema

val logDf = spark.read.format("csv").     
                option("header", "true").  
                option("delimiter", "\t"). 
                schema(logSchema).        
                load("C:\\Users\\azerty\\Desktop\\tornik-map-20171006.10000.tsv") // vous pouvez spécifier ici le path de votre fichier log




//case class Person(key1:String,key2:String)
                
val filDF= logDf.filter($"url".contains("/map/1.0/slab/")).select("url")

val NetDF= filDF.withColumn("_tmp", split($"url", "\\/")).select(
  $"_tmp".getItem(0).as("col1"),
  $"_tmp".getItem(1).as("col2"),
  $"_tmp".getItem(2).as("col3"),
  $"_tmp".getItem(3).as("col4"),
  $"_tmp".getItem(4).as("col5"),
  $"_tmp".getItem(5).as("col6"),
  $"_tmp".getItem(6).as("col7"),
  $"_tmp".getItem(7).as("col8")

).drop("_tmp").select("col5","col7")







NetDF.show()





spark.udf.register("groupconcat",  GroupConcat)
 


import org.apache.spark.sql.functions._ 


NetDF.withColumn("uniqueID",monotonicallyIncreasingId).createOrReplaceTempView("parsetable")


val finalDF = spark.sql("select col5, count(*) as viewmodes, groupconcat(distinct col7 ) as zooms from (select parsetable.*, (row_number() over (order by uniqueID) - row_number() over (partition by col5 order by uniqueID)) as grp from parsetable     ) parsetable group by grp, col5 ")

finalDF.show(500)
finalDF.write.parquet("C:\\Users\\azerty\\Desktop\\logresult")


}
