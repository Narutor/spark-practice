package 基础练习.mysql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * description
 *
 * @author 漩涡鸣人 2020/03/24 11:54
 */
object writeMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("mysql")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val connectionProperties: Properties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "102030405060")
    val jdbcData: DataFrame = spark.read
      .jdbc("jdbc:mysql://localhost:3306/naruto", "naruto.people", connectionProperties)
    //jdbc:mysql://localhost:3306/test?user=root&amp;password=&amp;useUnicode=true&amp;characterEncoding=utf8&amp;autoReconnect=true
    val row: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(Array((5, "ss", 3), (6, "ss", 3), (7, "ss", 3), (8, "ss", 3)))
    import spark.implicits._

    val df: DataFrame = row.map(a => {
      people(a._1, a._2, a._3)
    }).toDF()

    df.write.mode(SaveMode.Append)
      .jdbc("jdbc:mysql://localhost:3306/naruto",
        "naruto.people", connectionProperties)
  }
}

case class people(id: Int, user_name: String, password: Int)