package day0119

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object CsvSource {
  def main(args: Array[String]): Unit = {
    //1 创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("CsvSource")
      .master("local[2]").getOrCreate()
    //2 读取csv数据
    val dataFrame: DataFrame = sparkSession.read.csv("/Users/tosin/Documents/IdeaProjects/spark-test/test-data/saveCsv")
    //3 处理数据 定义列名
    val dataFrame1: DataFrame = dataFrame.toDF("id","xueyuan")
    import sparkSession.implicits._
    val dataSet: Dataset[Row] = dataFrame1.filter($"id" like("%ja%"))
    dataSet.show()
    //4 关闭资源
    sparkSession.stop()
  }
}
