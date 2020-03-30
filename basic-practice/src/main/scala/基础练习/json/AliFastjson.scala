package 基础练习.json

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * description
 *
 * @author 漩涡鸣人 2020/03/24 11:26
 */
object AliFastjson {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("text").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val data: RDD[String] = spark.sparkContext.textFile("D:\\test\\SparkStreaming\\src\\main\\resources\\yesy.json")

    data.map(
      a => {
        val data: JSONObject = JSON.parseObject(a)
        val dataw: String = data.getString("data")
        JSON.parseObject(dataw).size()
      }
    ).foreach(println)
  spark.stop()
  }
}
