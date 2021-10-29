1. ExampleWrite 写数据, 代码里吗路径写的是我的，使用时替换下就可以
2. ExampleRead 读数据，代码里吗路径写的是我的，使用时替换下就可以

### 提交作业
```shell
#  在EMR上下载hudi-spark3-bundle 0.9
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3-bundle_2.12/0.9.0/hudi-spark3-bundle_2.12-0.9.0.jar

# spark-sql的包，emr上和开源的不完全一样，其中 org.apache.spark.sql.execution.datasources.PartitionedFile参数不一样
# 读取MOR表的时候会有NoSuchMethodError错误, 将 emr 上使用spark-sql的包替换成开源的可以解决，但这里要说明的是这不是最佳实践。
# emr上的spark和开源是不一样的，emr的hudi是基于emr的spark编译的。现在要在emr上用开源的hudi-0.9,这个是在开源的spark上编译的，所以
# 在EMR运行可能会存在未知问题。最好等emr支持了hudi-0.9再使用。

# 下载开源的spark-sql，替换emr上的spark-sql


mv /usr/lib/spark/jars/spark-sql_2.12-3.1.1-amzn-0.1.jar /usr/lib/spark/jars/spark-sql_2.12-3.1.1-amzn-0.1.jar.1
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.1.1/spark-sql_2.12-3.1.1.jar
mv spark-sql_2.12-3.1.1.jar /usr/lib/spark/jars/


# 提交作业
spark-submit \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--jars ./hudi-spark3-bundle_2.12-0.9.0.jar \
--class com.aws.analytics.ExampleRead \
../emr-hudi-090-1.0-SNAPSHOT-jar-with-dependencies.jar

```

