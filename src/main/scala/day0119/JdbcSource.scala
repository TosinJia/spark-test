package day0119

import org.apache.spark.sql._

/**
  * 数据源：本地、hdfs mysql
  * mysql作为数据源
  * schema信息
  *   root
  *   |-- uid: integer (nullable = false)
  *   |-- xueyuan: string (nullable = true)
  *   |-- number_one: string (nullable = true)
  * */
object JdbcSource {
  def main(args: Array[String]): Unit = {
    //使用sparkSQL 1 创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("JdbcSource")
      .master("local[2]").getOrCreate()

    //2 加载数据源
    val dataFrame: DataFrame = sparkSession.read.format("jdbc")
      .options(Map(
        "url" -> "jdbc:mysql://192.168.0.114:3306/test",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable"->"url_data",
        "user" -> "root",
        "password" -> "TosinJia_1"
      )).load()
    //测试 打印schema
    dataFrame.printSchema()
    // 触发打印
    dataFrame.show()
    /**
      * 3 过滤数据 传递函数方式
      * */

    val filterDataSet: Dataset[Row] = dataFrame.filter(x => {
      //uid>2
      x.getAs[Int](0) > 2
    })
    filterDataSet.show()
    /**
      * 过滤数据 其他方式SQL
      * */
    import sparkSession.implicits._
    val filterDataSet2: Dataset[Row] = dataFrame.filter($"uid" > 2)
    val filterSelectDataFrame: DataFrame = filterDataSet2.select($"xueyuan",$"number_one")
    filterSelectDataFrame.show()

    /**
      * 结果保存
      * */
    //1. 写入text格式 Text data source supports only a single column, and you have 2 columns.;
    val filterSelect1DataFrame: DataFrame = filterDataSet2.select($"xueyuan")
    //filterSelect1DataFrame.write.text("/Users/tosin/Documents/IdeaProjects/spark-test/test-data/saveText")

    //2.写入json格式
    //filterSelectDataFrame.write.json("/Users/tosin/Documents/IdeaProjects/spark-test/test-data/saveJson")

    //3. 写入csv格式
    //filterSelectDataFrame.write.csv("/Users/tosin/Documents/IdeaProjects/spark-test/test-data/saveCsv")

    //4. 写入parquet格式 只读数据源 基于列式的压缩 节约资源
    filterSelectDataFrame.write.parquet("/Users/tosin/Documents/IdeaProjects/spark-test/test-data/saveParquet")
    //关闭资源
    sparkSession.stop()
  }
}
