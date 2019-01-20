package day0119

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JsonSource {
  def main(args: Array[String]): Unit = {
    // 1 创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("JsonSource")
      .master("local[2]").getOrCreate()
    // 2 读取json数据源
    val dataFrame: DataFrame = sparkSession.read.json("/Users/tosin/Documents/IdeaProjects/spark-test/test-data/saveJson")
    // 3 处理数据
    import sparkSession.implicits._
//    val dataSet: Dataset[Row] = dataFrame.filter($"xueyuan"==="java")
    val dataSet: Dataset[Row] = dataFrame.filter($"xueyuan" like("%ja%"))
    // 4 触发action
    dataSet.show()
    // 5 关闭资源
    sparkSession.stop()
  }
}
