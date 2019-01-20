package day0118

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlWordCount {
  def main(args: Array[String]): Unit = {
    //1 创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSqlWordCount")
      .master("local[2]").getOrCreate()
    //2 加载数据 使用DataSet处理数据
    val dataSet: Dataset[String] = sparkSession.read
      .textFile("/Users/tosin/Documents/IdeaProjects/spark-test/test-data/wc.txt")
    //dataSet是一个更加智能的rdd，默认有一列叫value，value存储的是所有数据
    dataSet.show()

    //3 sparksql 注册表registerTempTable/注册视图 rdd.flatMap
    import sparkSession.implicits._
    val wordDataSet: Dataset[String] = dataSet.flatMap(_.split(" "))

    //4 创建视图
    wordDataSet.createTempView("wc_t")

    //5 执行sql wordcount 别名（as） word
    val dataFrame: DataFrame = sparkSession.sql("select value word,count(*) sum from wc_t group by value order by sum desc")

    dataFrame.show()

    //6 关闭资源
    sparkSession.stop()

  }
}
