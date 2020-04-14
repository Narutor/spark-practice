package 基础练习.json

import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession}
/**
 * description
 *
 * @author 漩涡鸣人 2020/04/06 23:17
 */
object SparkJon {
  val json2: String = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"default\":0,\"field\":\"create_date\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"tenant_id\"}],\"optional\":true,\"name\":\"server287.hdsp_test.dev_test_demo_0210.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"default\":0,\"field\":\"create_date\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"tenant_id\"}],\"optional\":true,\"name\":\"server287.hdsp_test.dev_test_demo_0210.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"}],\"optional\":false,\"name\":\"server287.hdsp_test.dev_test_demo_0210.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":10013,\"name\":\"cxk227\",\"create_date\":1585796265000,\"tenant_id\":1},\"source\":{\"version\":\"1.0.0.Final\",\"connector\":\"mysql\",\"name\":\"server287\",\"ts_ms\":1585796265000,\"snapshot\":\"false\",\"db\":\"hdsp_test\",\"table\":\"dev_test_demo_0210\",\"server_id\":223344,\"gtid\":null,\"file\":\"mysql-bin.000014\",\"pos\":565195560,\"row\":0,\"thread\":1084258,\"query\":\"INSERT INTO `dev_test_demo_0210` (`name`, `tenant_id`) VALUES ('cxk227', '1')\"},\"op\":\"c\",\"ts_ms\":1585796356264}}"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").getOrCreate()
//    val a: Dataset[String] = qaDF.select("json").as[String]
    import spark.implicits._
    val jsonDf: Dataset[String] = spark.read.textFile("D:\\Git\\漩涡鸣人练习专用文件夹\\spark-practice\\basic-practice\\src\\test\\resources\\jsonData.json")
//    val df1 =spark.read.json(jsonDf.select("value").as[String])
    val b: Dataset[String] = jsonDf.select("value").as[String]
    val df1 =spark.read.json(b)
//    jsonDf.show(false)
    df1.show(false)

  }
}
