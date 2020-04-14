package 基础练习.writefile

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * description
 *
 * @author 漩涡鸣人 2020/04/15 0:01
 */
object SparkWriteToFile {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\InstallationPath\\hadoop-3.1.3")
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
//    basic-practice/src/main/resources/jsonData.json 4. Path From Repository Root
    val testDF: DataFrame = spark.read.json("basic-practice/src/test/resources/jsonData3.json")

    testDF.show(false)
    testDF.write.json("basic-practice/src/test/resources/jsonData4")
   /* testDF.write.mode("Append").csv("basic-practice/src/test/resources/jsonData4.csv")
    testDF.write.json("basic-practice/src/test/resources/jsonData4.json")
    //repartition 解决小文件问题
    testDF.repartition(1).write.text("basic-practice/src/test/resources/jsonData4.txt")
    testDF.write.format("parquet").save("basic-practice/src/test/resources/namesAndAges.parquet")
*/
  }
}
