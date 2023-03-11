package com.manjesh

import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.SparkSession


object HudiSparkSimple {
  val tableName = "hudi_trips_cow"
  val basePath = "file:///tmp/hudi_trips_cow"
  val dataGen = new DataGenerator

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("hudi-datalake")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(HoodieWriteConfig.TABLE_NAME, tableName).
      mode(Append).
      save(basePath)
    print(" **** Table created **** ")
    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath)
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot").show()
  }

}