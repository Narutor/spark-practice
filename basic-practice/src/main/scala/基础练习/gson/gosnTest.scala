package 基础练习.gson

import com.google.gson.{JsonElement, JsonParser}

/**
 * description
 *
 * @author 漩涡鸣人 2020/04/02 14:31
 */
object gosnTest {
  val json = "{\n  \"syncSpark\": {\n    \"sparkAppName\": \"dev_test_demo0210\",\n    \"interval\": 1,\n    \"writeType\": \"jdbc\"\n  },\n  \"syncColumns\": [\n    {\n      \"colIndex\": 1,\n      \"typeName\": \"number\",\n      \"colName\": \"id\"\n    },\n    {\n      \"colIndex\": 2,\n      \"typeName\": \"string\",\n      \"colName\": \"name\"\n    },\n    {\n      \"colIndex\": 3,\n      \"typeName\": \"date\",\n      \"colName\": \"create_date\"\n    }\n  ],\n  \"syncFile\": {\n    \"metastoreUris\": \"thrift://hdsp001:9083\",\n    \"hiveDatabaseName\": \"test\",\n    \"hiveTableName\": \"write_test_people\",\n    \"format\": \"csv\",\n    \"writeMode\": \"append\",\n    \"writePath\": \"hdfs://hdsp001:8020/warehouse/tablespace/managed/hive/test.db/write_test_people\",\n    \"checkpointLocation\": \"check/path04\"\n  },\n  \"syncKafka\": {\n    \"kafkaBootstrapServers\": \"hdsp001:6667,hdsp002:6667,hdsp003:6667\",\n    \"kafkaTopic\": \"server287.hdsp_test.dev_test_demo_0210\",\n    \"initDefaultOffset\": \"latest\"\n  },\n  \"syncRedis\": {\n    \"redisHost\": \"hdsp004\",\n    \"redisPort\": 6379,\n    \"redisPassword\": \"hdsp_dev\",\n    \"redisDataBase\": 15\n  },\n  \"syncJdbc\": {\n    \"dbType\": \"MYSQL\",\n    \"pk\": \"id\",\n    \"saveMode\": \"upsert\",\n    \"driver\": \"com.mysql.jdbc.Driver\",\n    \"jdbcUrl\": \"jdbc:mysql://dev.hdsp.hand.com:7233/hdsp_test?useUnicode=true&characterEncoding=utf-8&useSSL=false\",\n    \"user\": \"hdsp_dev\",\n    \"pwd\": \"hdsp_dev\",\n    \"schema\": \"hdsp_test\",\n    \"table\": \"dev_test_demo_0210_copy\"\n  }\n}"
  val json2 = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"default\":0,\"field\":\"create_date\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"tenant_id\"}],\"optional\":true,\"name\":\"server287.hdsp_test.dev_test_demo_0210.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"default\":0,\"field\":\"create_date\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"tenant_id\"}],\"optional\":true,\"name\":\"server287.hdsp_test.dev_test_demo_0210.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"}],\"optional\":false,\"name\":\"server287.hdsp_test.dev_test_demo_0210.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":10013,\"name\":\"cxk227\",\"create_date\":1585796265000,\"tenant_id\":1},\"source\":{\"version\":\"1.0.0.Final\",\"connector\":\"mysql\",\"name\":\"server287\",\"ts_ms\":1585796265000,\"snapshot\":\"false\",\"db\":\"hdsp_test\",\"table\":\"dev_test_demo_0210\",\"server_id\":223344,\"gtid\":null,\"file\":\"mysql-bin.000014\",\"pos\":565195560,\"row\":0,\"thread\":1084258,\"query\":\"INSERT INTO `dev_test_demo_0210` (`name`, `tenant_id`) VALUES ('cxk227', '1')\"},\"op\":\"c\",\"ts_ms\":1585796356264}}"

  def main(args: Array[String]): Unit = {
    val gsonString: JsonElement = JsonParser.parseString(json2)
    val payload: JsonElement = gsonString.getAsJsonObject.get("payload")
    val after: JsonElement = payload.getAsJsonObject.get("after")

    println(after)
    val id: JsonElement = after.getAsJsonObject.get("id")
    println(id)
    val op: JsonElement = payload.getAsJsonObject.get("op")
    println(op)

  }
}
