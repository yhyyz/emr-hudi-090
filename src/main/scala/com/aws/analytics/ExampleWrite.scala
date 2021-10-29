package com.aws.analytics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

object ExampleWrite {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("ExampleWrite")
//      .setMaster("local[6]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.cbo.enabled", "true")
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val tableName = "hudi_090_1"
    val basePath = "s3a://app-util/hudi_mor_090_1"

    val str="""{"ts": 1633565019672, "uuid": "b8c03c1d-b08c-4aef-8ff3-02499a149eaws4", "name": "aws4"}"""
    import scala.collection.JavaConverters._
    val inserts = List(str).asJava

    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(RECORDKEY_FIELD.key(), "uuid").
      option(PARTITIONPATH_FIELD.key(),"").
      option("hoodie.insert.shuffle.parallelism","3").
      option("hoodie.upsert.shuffle.parallelism","3").
      option("hoodie.cleaner.commits.retained","1").
      option("hoodie.keep.min.commits","2").
      option("hoodie.keep.max.commits","3").
      option("hoodie.parquet.small.file.limit", "0").
      option("hoodie.datasource.write.table.type", "MERGE_ON_READ").
      option(TBL_NAME.key(), tableName).
      mode(Overwrite).
      save(basePath)
  }

}
