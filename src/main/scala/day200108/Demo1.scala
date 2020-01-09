package day200108

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * sparksql 操作hive
 * 1. 连接hive
 * 2. 读取hive中的数据
 * 3. 写进mysql中
 * 打包上传集群运行，master()不用写
 */
object Demo1 {
  def main(args: Array[String]): Unit = {
    //sparksession可以直接连接到hive .master("local") 本地没有hive
    val spark: SparkSession = SparkSession.builder().appName("hive2mysql").enableHiveSupport().getOrCreate()

    //性能优化相关参数 只对次应用程序有效
//    val spark: SparkSession = SparkSession.builder().appName("hive2mysql").enableHiveSupport()
//      .config("spark.sql.autoBroadcastJoinThreshold", "50").getOrCreate()

    //读取数据 select wage from emp order by wage desc
    val result: DataFrame = spark.sql(args(0))

    //将结果写入关系数据库，MYSQL
    //连接mysql
    val pro: Properties = new Properties()
    pro.setProperty("user", "root")
    pro.setProperty("password", "000000")
    pro.setProperty("driver", "com.mysql.jdbc.Driver")

    //将查询结果写入MySQL中
    result.write.jdbc("jdbc:mysql://bd-01-01:3306/company?serverTimezone=UTC&characterEncoding=utf-8", args(1), pro)

    spark.stop()
  }
}
