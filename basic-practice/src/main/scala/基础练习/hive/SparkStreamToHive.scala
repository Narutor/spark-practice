package 基础练习.hive

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

/**
 * description
 *
 * @author 漩涡鸣人 2020/03/24 11:46
 */
object SparkStreamToHive {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("text")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")

    val session: SparkSession = SparkSession.builder()
      .config(conf)
      // 指定hive的metastore的端口  默认为9083 在hive-site.xml中查看
      .config("hive.metastore.uris", "thrift://naruto:9083")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://naruto:8020/user/hive/warehouse")
      //直接连接hive
      .enableHiveSupport()
      .getOrCreate()

    val ssc = new StreamingContext(session.sparkContext, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "sasuky:9092,naruto:9092,sakura:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics: Array[String] = Array("testTopic")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.
    val result = stream.foreachRDD(rdd => {
      //      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //获取到分区和偏移量信息
      val events: RDD[Some[String]] = rdd.map(x => {
        val data: String = x.value()
        Some(data)
      })
      // 开启动态分区
      session.sql("set hive.exec.dynamic.partition=true")
      session.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      val dataRow: RDD[Row] = events.map(line => {
        val temp: Array[String] = line.get.split(" ")
        Row(temp(0), temp(1), temp(2), temp(3))
      })
      val structType: StructType = StructType(Array(
        StructField("first", StringType, true),
        StructField("second", StringType, true),
        StructField("third", StringType, true),
        StructField("fourth", StringType, true)
      ))

      val df: DataFrame = session.createDataFrame(dataRow, structType)
      //写入hive
      df.write.mode(SaveMode.Append)
        .format("parquet")
        .insertInto("test.test_demo")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
