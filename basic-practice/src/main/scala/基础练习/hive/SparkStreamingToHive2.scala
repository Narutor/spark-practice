package 基础练习.hive

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


/**
 * description
 *
 * @author 漩涡鸣人 2020/03/24 22:54
 */
object SparkStreamingToHive2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf: SparkConf = new SparkConf().setAppName("text")
      .setMaster("local[*]")
    //序列化 rdd压缩
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
    //
    val session = SparkSession.builder()
      .config(conf)
      // 指定hive的metastore的端口  默认为9083 在hive-site.xml中查看
      .config("hive.metastore.uris", "thrift://hdsp001:9083")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://hdsp001:8020/warehouse")
      //直接连接hive
      .enableHiveSupport()
      .getOrCreate()

    session.sql("show databases").show(false)
    //此处创建StreamingContext 必须在 session 后面 否则 只能查出来一个库
    val ssc = new StreamingContext(session.sparkContext, Seconds(5))

    //kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hdsp001:6667,hdsp002:6667,hdsp003:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("server338.hdsp_test.source_user_072")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    import session.implicits._
    val offset: DStream[Long] = stream.map(_.offset())
    offset.print()

    val value: DStream[String] = stream.map(_.value())
    println(value.toString)
    /*val result = stream.foreachRDD(rdd => {

      val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //获取到分区和偏移量信息
      /*  val events: RDD[Some[String]] = rdd.map(x => {
          val data = x.value()
          Some(data)
        })*/
      rdd.foreach(println)

      /*   session.sql("set hive.exec.dynamic.partition=true")
         session.sql("set hive.exec.dynamic.partition.mode=nonstrict")
   */
      /*  val dataRow = events.map(line => {
          val temp = line.get.split(" ")
          Row(temp(0), temp(1), temp(2), temp(3))
        })*/
      //structType
      /*  val structType = StructType(Array(
          StructField("first", StringType, true),
          StructField("second", StringType, true),
          StructField("third", StringType, true),
          StructField("fourth", StringType, true)
        ))*/
      //创建表
      //      val df: DataFrame = session.createDataFrame(dataRow, structType)
      //      df.write.mode(SaveMode.Append).format("parquet").insertInto("test.test_demo")
      //      )
    })*/

    ssc.start()
    ssc.awaitTermination()
  }
  }