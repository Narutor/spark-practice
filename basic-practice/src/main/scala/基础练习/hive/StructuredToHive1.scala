package 基础练习.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * description
 *  通过Structured Streaming写入文件的方式，写入hive
 *
 * @author 漩涡鸣人 2020/03/24 11:39
 */
object StructuredToHive1 {
  def main(args: Array[String]): Unit = {

    // 如果hadoop没有启Kerberos或者从Kerberos获取的用户为null，那么获取HADOOP_USER_NAME环境变量，
    // 并将它的值作为Hadoop执行用户。如果我们没有设置HADOOP_USER_NAME环境变量，那么程序将调用whoami来获取当前用户，并用groups来获取用户所在组
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    // Spark默认将用户程序运行所在系统的当前登录用户作为用户程序的关联用户
    System.setProperty("user.name", "hdfs")

    val conf: SparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("structured_streaming_test")
      // 加这个配置访问集群中的hive
      //     https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
      .set("spark.sql.warehouse.dir", "/user/hive")
      .set("metastore.catalog.default", "hive")
      .set("hive.metastore.uris", "thrift://naruto:9083")

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .config("dataSource", "test_table")
      .enableHiveSupport()
      .getOrCreate()

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sasuky:9092,naruto:9092,sakura:9092")
      .option("subscribe", "mytest.just_test.book")
      .option("failOnDataLoss", value = false)
      .option("subscribe", "testTopic")
      .load()
    import spark.implicits._

    val res: DataFrame = df.selectExpr("CAST(value AS STRING)")
      .as[String].map(a => {
      val da: Array[String] = a.split(" ")
      test(da(0), da(1), da(2), da(3))
    }).toDF()

    /*  val query = res
       .writeStream
       .format("console")
       .outputMode("append")
       .start()*/

    val query: StreamingQuery = res
      //降低并行度 解决小文件问题
      .repartition(1)
      .writeStream
      .format("parquet")
      .outputMode("append")
      //写入到hive文件存储路径下
      .option("path", "hdfs://naruto:8020/user/hive/warehouse/test.db/test_demo")
      .option("checkpointLocation", "outdir3")
      .start()

    query.awaitTermination()
  }
}

case class test(first: String, second: String, third: String, fourth: String) //定义字段名和类型