package 基础练习.dynamicRow

import com.google.gson.{JsonElement, JsonParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SparkSession, _}

/**
 * description
 * 解析mysql debezium 数据
 * 动态返回Row
 *
 * @author 漩涡鸣人 2020/04/02 16:48
 */
object DynamicRow {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val jsondata: RDD[String] = spark.sparkContext.textFile("D:\\Git\\漩涡鸣人练习专用文件夹\\spark-practice\\basic-practice\\src\\test\\resources\\jsonData.json")
    val array: Array[String] = Array("id", "name", "create_date")
    //广播变量
    val arrayBroadcast: Broadcast[Array[String]] = spark.sparkContext.broadcast(array)

    val after: RDD[Row] = jsondata.map(value => {
      var seq: Seq[String] = Seq[String]()
      val payload: JsonElement = JsonParser.parseString(value).getAsJsonObject.get("payload")
      if (payload.getAsJsonObject.get("after") != null && payload.getAsJsonObject.get("after").toString.length > 0 && payload.getAsJsonObject.get("after").toString != "null") {
        val op: String = payload.getAsJsonObject.get("op").toString
        val ts_ms: String = payload.getAsJsonObject.get("ts_ms").toString
        seq = seq :+ op :+ ts_ms
        arrayBroadcast.value.foreach(a => {
          seq = seq :+ payload.getAsJsonObject.get("after").getAsJsonObject.get(a).toString
        })
        //用seq动态返回Row
        Row.fromSeq(seq)
      }
      else {
        val op: String = payload.getAsJsonObject.get("op").toString
        seq = seq :+ op
        arrayBroadcast.value.foreach(a => {
          seq = seq :+ payload.getAsJsonObject.get("before").getAsJsonObject.get(a).toString
        })
        Row.fromSeq(seq)
      }
    })

    after.foreach(println)

    //动态写StructType
    var dataStructType: StructType = new StructType()
      .add("op", StringType, false)
      .add("ts_ms", StringType, false)
    for (elem <- array) {
      dataStructType = dataStructType.add(elem, StringType, false)
    }

    println(dataStructType)

    val daa: DataFrame = spark.createDataFrame(after, dataStructType)
    daa.show(false)
  }
}
