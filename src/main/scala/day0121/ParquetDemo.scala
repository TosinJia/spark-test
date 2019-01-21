package day0121

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 读取parquet数据源
  * 做了压缩，提高程序的运行效率
  *
  * MR：压缩
  * Hive：压缩
  * 程序优化
  * */
object ParquetDemo {
  def main(args: Array[String]): Unit = {
    // 1 创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("ParquetDemo")
      .master("local[2]").getOrCreate()
    // 2 读取parquet数据
    val dataFrame: DataFrame = sparkSession.read.parquet("/Users/tosin/Documents/IdeaProjects/spark-test/test-data/saveParquet")
    // 3 带有schema信息
    dataFrame.printSchema()
    dataFrame.show()

    // 4 计算
    import sparkSession.implicits._
    val dataSet: Dataset[Row] = dataFrame.filter($"xueyuan" like("%ja%"))

    dataSet.show()
    // 关闭资源
    sparkSession.stop()
  }
}
