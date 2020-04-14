package 基础练习.json

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * description
 *
 * @author 漩涡鸣人 2020/04/07 13:15
 */
object AudioJson {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val rddData = spark.read.option("multiLine", true).textFile("D:\\Git\\漩涡鸣人练习专用文件夹\\spark-practice\\basic-practice\\src\\test\\resources\\jsonData2.json")
    rddData.show(false)
    import spark.implicits._

    val value = rddData.select("value").as[String]

   /* val df1 = spark.read.option("multiLine", true).json(value)

    val df3 = df1.drop($"data.E037")
    df1.drop($"data").show(false)

    //dropDuplicates
    val df4 = df1.drop("data.E037").show(false)
    val data = df1.select("data").as[String]
    val da5= spark.read.option("multiLine", true).json(data)
    da5.show(false)*/


    //      .show(false)
    //    data.show(false)

    /* val df2 =  df1.select($"data.E037")
      val df3 =df1.drop($"data.E037")
      df3.show(false)
      val df4 =  df3.select($"data.E037")
      df4.show(false)*/
    //    dataDf2.show(false)
    //    val dataDf: DataFrame = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json(value)
    /*val data: Dataset[String] = rddData.select("data").as[String]
    data.show(false)
    val dataDf2: DataFrame =spark.read.json(data)
    dataDf2.show(false)
    val dataDf3= dataDf2.select("_corrupt_record").as[String]
    val dataDf4=spark.read.json(dataDf3)
      .show(false)*/
  }
}
