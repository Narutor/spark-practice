package 基础练习.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * description
 *
 * @author 漩涡鸣人 2020/04/07 11:12
 */
object StremingJson {
  def main(args: Array[String]): Unit = {

    val jsonData="{\"id\":\"a55b2d90-691e-4532-8d01-eff3b085a66e\",\"vin\":\"d69362f8ba6a27d496\",\"brand\":\"FAW_VW\",\"eventCode\":\"DRIVETIMER\",\"messageType\":\"NatStd_StateMsg\",\"sendType\":\"1\",\"data\":{\"E037\":{\"sts\":0,\"val\":\"7.4\"},\"V051\":{\"sts\":0,\"val\":\"1\"},\"V050\":{\"sts\":0,\"val\":\"1\"},\"V015\":{\"sts\":0,\"val\":\"5667.0\"},\"E060\":{\"sts\":0,\"val\":\"1\"},\"E061\":{\"sts\":0,\"val\":\"1\"},\"E062\":{\"sts\":0,\"val\":\"19.0\"},\"E063\":{\"sts\":0,\"val\":\"1\"},\"E064\":{\"sts\":0,\"val\":\"2\"},\"E065\":{\"sts\":0,\"val\":\"18.0\"},\"A038\":{\"sts\":0,\"val\":\"0\"},\"E066\":{\"sts\":0,\"val\":\"1\"},\"A039\":{\"sts\":0,\"val\":\"0\"},\"A037\":{\"sts\":0,\"val\":\"0\"},\"E026\":{\"sts\":0,\"val\":[{\"sts\":0,\"val\":1},{\"sts\":0,\"val\":\"1\"},{\"sts\":0,\"val\":\"3\"},{\"sts\":0,\"val\":\"23.0\"},{\"sts\":0,\"val\":\"0.0\"},{\"sts\":0,\"val\":\"0.0\"},{\"sts\":0,\"val\":\"46.0\"},{\"sts\":0,\"val\":\"368.3\"},{\"sts\":0,\"val\":\"0.0\"}]},\"GPS010\":{\"sts\":0,\"val\":\"0\"},\"V049\":{\"sts\":0,\"val\":\"1\"},\"V048\":{\"sts\":0,\"val\":\"1\"},\"P001\":{\"sts\":0,\"val\":\"2\"},\"E010\":{\"sts\":0,\"val\":\"0.01\"},\"E054\":{\"sts\":0,\"val\":\"1\"},\"A049\":{\"sts\":0,\"val\":\"0\"},\"E055\":{\"sts\":0,\"val\":\"13\"},\"E056\":{\"sts\":0,\"val\":\"3.6\"},\"A047\":{\"sts\":0,\"val\":\"0\"},\"E057\":{\"sts\":0,\"val\":\"1\"},\"A048\":{\"sts\":0,\"val\":\"0\"},\"E058\":{\"sts\":0,\"val\":\"56\"},\"A045\":{\"sts\":0,\"val\":\"0\"},\"E059\":{\"sts\":0,\"val\":\"3.595\"},\"A046\":{\"sts\":0,\"val\":\"0\"},\"E015\":{\"sts\":0,\"val\":[{\"val\":\"1.0\",\"sts\":0},{\"val\":\"1.0\",\"sts\":0},{\"val\":\"368.5\",\"sts\":0},{\"val\":\"-3.0\",\"sts\":0},{\"val\":\"104.0\",\"sts\":0},{\"val\":\"1.0\",\"sts\":0},{\"val\":\"104.0\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.595\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.535\",\"sts\":0},{\"val\":\"3.535\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.535\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.539\",\"sts\":0},{\"val\":\"3.537\",\"sts\":0},{\"val\":\"3.536\",\"sts\":0},{\"val\":\"3.538\",\"sts\":0},{\"val\":\"16.0\",\"sts\":0},{\"val\":\"19.0\",\"sts\":0},{\"val\":\"18.0\",\"sts\":0},{\"val\":\"18.0\",\"sts\":0},{\"val\":\"18.0\",\"sts\":0},{\"val\":\"19.0\",\"sts\":0},{\"val\":\"18.0\",\"sts\":0},{\"val\":\"19.0\",\"sts\":0},{\"val\":\"18.0\",\"sts\":0},{\"val\":\"19.0\",\"sts\":0},{\"val\":\"18.0\",\"sts\":0},{\"val\":\"19.0\",\"sts\":0},{\"val\":\"18.0\",\"sts\":0},{\"val\":\"19.0\",\"sts\":0},{\"val\":\"19.0\",\"sts\":0},{\"val\":\"19.0\",\"sts\":0},{\"val\":\"18.0\",\"sts\":0}]},\"A043\":{\"sts\":0,\"val\":\"0\"},\"A044\":{\"sts\":0,\"val\":\"0\"},\"A041\":{\"sts\":0,\"val\":\"0\"},\"A042\":{\"sts\":0,\"val\":\"0\"},\"A040\":{\"sts\":0,\"val\":\"0\"},\"GPS009\":{\"sts\":0,\"val\":\"0\"},\"GPS008\":{\"sts\":0,\"val\":\"0\"},\"GPS003\":{\"sts\":0,\"val\":\"36.25211\"},\"GPS002\":{\"sts\":0,\"val\":\"120.042222\"},\"V019\":{\"sts\":0,\"val\":\"0.0\"},\"V018\":{\"sts\":0,\"val\":\"P\"},\"E001\":{\"sts\":0,\"val\":\"3\"},\"E003\":{\"sts\":0,\"val\":\"1\"},\"A056\":{\"sts\":0,\"val\":\"0\"},\"E004\":{\"sts\":0,\"val\":\"8625\"},\"E049\":{\"sts\":0,\"val\":\"0\"},\"A054\":{\"sts\":0,\"val\":\"0\"},\"A055\":{\"sts\":0,\"val\":\"0\"},\"A052\":{\"sts\":0,\"val\":\"0\"},\"E008\":{\"sts\":0,\"val\":\"368.5\"},\"A053\":{\"sts\":0,\"val\":\"0\"},\"E009\":{\"sts\":-1,\"val\":\"-3.0\"},\"A050\":{\"sts\":0,\"val\":\"0\"},\"A051\":{\"sts\":0,\"val\":\"0\"}},\"recvTs\":null,\"pkgTs\":1553833126000,\"model\":\"Audi A6 / C7 China\"}"
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf: SparkConf = new SparkConf().setAppName("text")
      .setMaster("local[*]")
      //序列化 rdd压缩
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
    //
    val session = SparkSession.builder().config(conf).getOrCreate()

    //此处创建StreamingContext 必须在 session 后面 否则 只能查出来一个库
    val ssc = new StreamingContext(session.sparkContext, Seconds(5))

    //kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hdsp001:6667,hdsp002:6667,hdsp003:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streamer",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("naruto_test")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.map(record =>
      record.value()).foreachRDD(rdd => {
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      val ds: Dataset[String] = spark.createDataset(rdd)
      ds.show(false)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
