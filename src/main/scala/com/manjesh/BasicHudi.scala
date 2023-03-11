package com.manjesh

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.io.Directory


case class Employee(employeeId: Long, employeeName: String, employeeEmail: String, ts: Long)

object App {
  val basePath = "file:///Users/manjeshgowda/Documents/projects/sxm_projects/apache-hudi-essentials/store"
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def dateToLong(dateString: String): Long = LocalDate.parse(dateString, formatter).toEpochDay

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("hudi-datalake")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val employees = Seq(
      Employee(1, "Steve", "steve@github.com", dateToLong("2019-12-01")),
      Employee(2, "Dave", "dave@github.com", dateToLong("2019-12-01"))
    )
    import spark.implicits._
    val tableName = "tbl_employee"

    employees.toDF().write.
      format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "employeeId")
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(s"$basePath/$tableName")

    print("\n\n************* \n\n")
    print(s"$basePath/$tableName/*")
    print("\n\n************* \n\n")

    // spark.read.format("hudi").load(s"$basePath/$tableName/*").show()
    spark.read.format("hudi").load("/Users/manjeshgowda/Documents/projects/sxm_projects/apache-hudi-essentials/store/tbl_employee/*").show()
    print("************* \n\n")
  }
}