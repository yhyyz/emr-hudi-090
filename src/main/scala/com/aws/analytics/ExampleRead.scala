package com.aws.analytics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object ExampleRead {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("ExampleRead")
//      .setMaster("local[6]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.cbo.enabled", "true")
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val basePath = "s3a://app-util/hudi_mor_090_1"

    spark.
      read.
      format("hudi").
      load(basePath+"/*").
      createOrReplaceTempView("hudi_mor_090_1")
    spark.sql("select * from hudi_mor_090_1").show()
  }

}
