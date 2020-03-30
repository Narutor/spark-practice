package 基础练习.hbase

import org.apache.arrow.flatbuf.Date
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQuery
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import 基础练习.util.Jpools

/**
 * description
 *
 * @author 漩涡鸣人 2019/11/08 11:35
 */
object StructuredToHbase {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf: SparkConf = new SparkConf().
      setMaster("local[2]").
      setAppName("structured_streaming_hbase")
    val spark: SparkSession = getOrCreateSparkSession(conf)

    //从连接池获取连接
    val jedise: Jedis = Jpools.getJedis
    //从redis获取偏移量
    /* var lastSavedOffset: Int = 0
     if (jedise.get("testTopic") == null) {
       lastSavedOffset = -1
     } else {
       lastSavedOffset = jedise.get("testTopic").toInt + 1
       print(lastSavedOffset)
     }*/

    val dataFromKafka: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sasuky:9092,naruto:9092,sakura:9092")
      // 提交模式 assign {"topicA":[0,1],"topicB":[2,4]}   subscribe 多个主题用逗号分开
      // topic，如果两个以上topic用逗号隔开
      .option("subscribe", "my-null-topic")
      //startingOffsets  -2指代最早 -1 指最新
      //endingOffsets {“topicA”：{“0”：23，“1”：-1}，“topicB”：{“0”：-1}}  json参数第一个分区，第二个偏移量
      //.option("startingOffsets", """{ """" + "my-null-topic" + """":{"0":""" + -1 + """}}""")
      .load()

    //从kafka拉取数据后，选择需要的内容
    val frame: DataFrame = dataFromKafka.select(functions.col("value").cast("String"),
      //      functions.col("timestamp").cast("timestamp")
      functions.col("offset").cast("Long")
    )
      .select("value", "offset")

    /* //打印到控制台
    val query = frame
       .writeStream
       .format("console")
       .outputMode("complete")
       .start()*/

    var jedis: Jedis = null

    val query: StreamingQuery = frame
      .writeStream
      .outputMode("append")
      //foreachsick 自定义数据源 效率比较低
      .foreach(new ForeachWriter[Row]() {
        override def open(partitionId: Long, version: Long): Boolean = {
          val config: JedisPoolConfig = new JedisPoolConfig()
          config.setMaxTotal(20)
          config.setMaxIdle(5)
          config.setMaxWaitMillis(1000)
          config.setMinIdle(2)
          config.setTestOnBorrow(false)
          val jedisPool = new JedisPool(config, "127.0.0.1", 6379)
          jedis = jedisPool.getResource
          val startDate = new Date()
          println("开始时间" + startDate)

          true
        }

        override def process(value: Row): Unit = {
          val sparkSession: SparkSession = getOrCreateSparkSession(conf)
          val rowRDD: RDD[Row] = sparkSession.sparkContext.makeRDD(Seq(value))
          import org.apache.hadoop.hbase.client.{Put, Result}
          import org.apache.hadoop.hbase.io.ImmutableBytesWritable
          import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
          import org.apache.hadoop.hbase.util.Bytes
          import org.apache.hadoop.mapreduce.Job
          /**
           * 创建HBase配置
           *
           * @return Configuration
           */
          def createHBaseConf: Configuration = {
            val conf = HBaseConfiguration.create()
            conf.set("hbase.zookeeper.quorum", "naruto,sasuky,sakura")
            conf.set("hbase.zookeeper.property.clientPort", "2181")
            //             conf.set("hbase.defaults.for.version.skip", "true")
            //            conf.set("zookeeper.znode.parent", "/hbase-unsecure")
            conf.set(TableOutputFormat.OUTPUT_TABLE, "test_table")
            conf
          }

          val confer: Configuration = createHBaseConf
          val job: Job = Job.getInstance(confer)
          job.setOutputKeyClass(classOf[ImmutableBytesWritable])
          job.setOutputValueClass(classOf[Result])
          job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
          rowRDD.map { record =>
            val rowId: String = record.get(1).toString
            val put = new Put(Bytes.toBytes(rowId))
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("value"), Bytes.toBytes(record.get(0).toString))
            (new ImmutableBytesWritable, put)
          }.saveAsNewAPIHadoopDataset(job.getConfiguration)
          //jedis.hincrBy("my-topic", value.get(0).toString, value.getLong(1))
          //jedis.getSet("my-topic", value.get(1).toString)
        }

        override def close(errorOrNull: Throwable): Unit = {
          //关闭连接

          jedis.close()
          val afterDate = new Date()
          println("结束时间" + afterDate)
        }
      })
      .start()
    query.awaitTermination()
  }

  def getOrCreateSparkSession(conf: SparkConf): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    spark
  }
}