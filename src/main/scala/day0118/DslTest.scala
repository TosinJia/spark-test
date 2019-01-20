package day0118

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * DSL风格
  * */
object DslTest {
  def main(args: Array[String]): Unit = {
    // 1 构建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("DslTest")
      .master("local[2]").getOrCreate()
    // 2 创建RDD
    val dataRdd: RDD[String] = sparkSession.sparkContext
      .textFile("/Users/tosin/Documents/IdeaProjects/spark-test/test-data/user.txt")
    // 3 切分数据
    val splitRdd: RDD[Array[String]] = dataRdd.map(_.split("\t"))
    // 4 API创建dataFrame spark-shell rdd转换为dataFrame

    val rowRdd: RDD[Row] = splitRdd.map(x => {
      val id: Int = x(0).toInt
      val name: String = x(1).toString
      val age: Int = x(2).toInt
      //Row代表一行数据
      Row(id, name, age)
    })
    val schema: StructType = StructType(List(
      //结构字段
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    val dataFrame: DataFrame = sparkSession.createDataFrame(rowRdd, schema)
    // 5 DSL风格 查询年龄大于18 RDD DataFrame DataSet（更加智能的分布式数据集）
    // 导入隐式
    import sparkSession.implicits._
    val dataSet: Dataset[Row] = dataFrame.where($"age">18)

    dataSet.show()
    // 6 关闭资源
    sparkSession.stop()
  }
}
