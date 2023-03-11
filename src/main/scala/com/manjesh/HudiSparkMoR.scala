package com.manjesh

import org.apache.hudi.QuickstartUtils._
import org.apache.spark.sql.SparkSession


object HudiSparkMoR {
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
    spark.sql(
      """
        |create table hudi_ctas_cow_nonpcf_tbl
        |using hudi
        |tblproperties (primaryKey = 'id')
        |as
        |select 1 as id, 'a1' as name, 10 as price;
        |""".stripMargin).show()
  }
}