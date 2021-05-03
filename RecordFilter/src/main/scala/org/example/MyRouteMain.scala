package org.example

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, concat, date_format, explode, expr, from_json, from_unixtime, lit, split, to_timestamp, unix_timestamp}
import org.example.schema.RecordClass
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructField, StructType}
/**
 * A Main to run MyRouteBuilder
 */
object MyRouteMain {

  def main(args: Array[String]) {
    if(args.length == 0 ){
     throw new IllegalArgumentException("Argument not specified")
    }
    new Processor(args).start()
  }

}

class Processor (args: Array[String]){

  def start(): Unit = {
    val inputFile =  args(0)
    val outputFile = args(1)
    val spark = SparkSession.builder.master("local[*]").appName("FilterRecord").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val schema = StructType(
      List(
        StructField("message", StringType, true)))

    var inputDf = spark.readStream
      .schema(schema)
      .json(inputFile)

    inputDf = inputDf.filter(col("message").contains("omwssu"))
    val replacedDf = inputDf.map(row => row.getString(0).replaceAll("\\s+"," "))
    var df = replacedDf.withColumn("tmp", split($"value", " "))
    df = df.withColumn("date",$"tmp".getItem(1)).withColumn("time",$"tmp".getItem(2)).withColumn("url",$"tmp".getItem(12))
    df = df.drop("tmp")
    df = df.withColumn("datetime",concat(col("date"), lit(" ") ,col("time")))
    df = df.withColumn("times2",to_timestamp($"datetime","yyyy-MM-dd HH:mm:ss")).withColumn("timestamp", from_unixtime(unix_timestamp(col("times2"), "yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))
    df = df.drop("date","time","datetime","times2")

    //details from the URL can be extracted using regex as below
    val urlPattern = "^(\\w+):\\/{2}(\\w*)\\.?([^\\/]*)([^\\?]*)\\??(/errors/)?".r
    val egUrl = "http://omwssu.lab5a.nl.dmdsdp.com/errors/0000F0-HZNSTB-123456789/devupdate/0/5/Testing%20by%20Selene%20Team"
    urlPattern.findAllIn(egUrl).matchData.foreach{x=> println(s"${x.group(1)} ${x.group(2)} ${x.group(3)} ${x.group(4)} ${x.group(5)}")}

    df.writeStream
      .format("json")
      .option("path", outputFile)
      .option("checkpointLocation", "D:\\New_folder\\checkpoint")
      .outputMode("append")
      .start().awaitTermination()


  }

}


