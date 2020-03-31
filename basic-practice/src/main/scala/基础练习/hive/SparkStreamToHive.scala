package 基础练习.hive

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
 * description
 *
 * @author 漩涡鸣人 2020/03/24 11:46
 */
object SparkStreamToHive {
  private val log = Logger(LoggerFactory.getLogger(SparkStreamToHive.getClass))
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hive")
    System.setProperty("user.name", "hive")
    val conf: SparkConf = new SparkConf().setAppName("text")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")

    val session: SparkSession = SparkSession.builder()
      .config(conf)
      // 指定hive的metastore的端口  默认为9083 在hive-site.xml中查看
      .config("hive.metastore.uris", "thrift://hdsp001:9083")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://hdsp001:8020/warehouse/")
      //直接连接hive
      .enableHiveSupport()
      .getOrCreate()

    val ssc = new StreamingContext(session.sparkContext, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hdsp001:6667,hdsp002:6667,hdsp003:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics: Array[String] = Array("test_demo01")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    val result = stream.foreachRDD(rdd => {
      //      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //获取到分区和偏移量信息
      val events: RDD[Row] = rdd.map(x => {
        val data: String = x.value()
        val arry = data.split(" ")
        Row(arry(0),arry(1),arry(2),arry(3))
      })
      // 开启动态分区
      session.sql("set hive.exec.dynamic.partition=true")
      session.sql("set hive.exec.dynamic.partition.mode=nonstrict")
     /* val dataRow: RDD[Row] = events.map(line => {
//        val temp: Array[String] = line.get.split(" ")
        Row("first", "second", "third", "fourth")
      })
      dataRow.foreach(println)*/
      val structType: StructType = StructType(Array(
        StructField("first", StringType, true),
        StructField("second", StringType, true),
        StructField("third", StringType, true),
        StructField("fourth", StringType, true)
      ))

      val df: DataFrame = session.createDataFrame(events, structType)
      df.show(false)
      //写入hive
     /* df.write.mode(SaveMode.Append)
        .format("parquet")
        .insertInto("test.test_demo01")*/
      //写入hive  这个方法可以自动创表
        df.write.mode("append")
          .format("parquet")
        .saveAsTable("test.test_demo01")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
