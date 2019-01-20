package day0118

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
/**
  * DSL风格 SQL风格
  * */
object JoinTest {
  def main(args: Array[String]): Unit = {
    //1 创建sparkSesion
    val sparkSession: SparkSession = SparkSession.builder().appName("JoinTest")
      .master("local[2]").getOrCreate()
    //2 直接创建DataSet
    import sparkSession.implicits._
    val dataSet1: Dataset[String] = sparkSession.createDataset(List(
      "1 tosin 18",
      "2 jacky 22",
      "3 tom 13"
    ))
    //3 整理数据
    val dataSet12: Dataset[(Int, String, Int)] = dataSet1.map(x => {
      val fields: Array[String] = x.split(" ")
      val id: Int = fields(0).toInt
      val name: String = fields(1).toString
      val age: Int = fields(2).toInt
      //元组输出
      (id, name, age)
    })
    //DataSet <--> DataFrame
    val dataFrame1: DataFrame = dataSet12.toDF("id","name","age")

    //第二份数据 第二张表
    val dataSet2: Dataset[String] = sparkSession.createDataset(List(
      "18 young",
      "22 old"
    ))
    val dataSet21: Dataset[(Int, String)] = dataSet2.map(x => {
      val fields: Array[String] = x.split(" ")
      val age: Int = fields(0).toInt
      val desc: String = fields(1).toString
      //元组输出
      (age, desc)
    })
    val dataFrame2: DataFrame = dataSet21.toDF("age2","desc")

    //DSL风格
    println("DSL风格")
    //默认方式 inner join
    val rDslDataFrame: DataFrame = dataFrame1.join(dataFrame2,$"age" === $"age2")
    rDslDataFrame.show()
    // left left_out 结果一样
    val rLeftDataFrame: DataFrame = dataFrame1.join(dataFrame2,$"age"===$"age2", "left")
    rLeftDataFrame.show()
    val rLeftOutDataFrame: DataFrame = dataFrame1.join(dataFrame2,$"age"===$"age2","left_outer")
    rLeftOutDataFrame.show()
    val rRightDataFrame: DataFrame = dataFrame1.join(dataFrame2,$"age"===$"age2","right")
    rRightDataFrame.show()
    println("CROSS")
    val rCrossDataFrame: DataFrame = dataFrame1.join(dataFrame2,$"age"===$"age2","cross")
    rCrossDataFrame.show()

    //SQL风格
    println("SQL风格")
    //4 创建视图
    dataFrame1.createTempView("t1")
    dataFrame2.createTempView("t2")

    //5 写sql（join）
    val rDataFrame: DataFrame = sparkSession.sql("select name,desc from t1 join t2 on t1.age=t2.age2")
    //6 触发任务
    rDataFrame.show()

    //关闭资源
    sparkSession.stop()
  }
}
