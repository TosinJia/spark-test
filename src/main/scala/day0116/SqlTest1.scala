package day0116

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * spark 2.x
  * sql风格 建议用
  * */
object SqlTest1 {
  def main(args: Array[String]): Unit = {
    //1. 构建SparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("SqlTest1").master("local[2]").getOrCreate()
    //2. 创建RDD  e:/temp/spark-user.txt /Users/tosin/Documents/IdeaProjects/spark-test/test-data/user.txt  hdfs://192.168.1.150:9000/testdata/spark/spark-user.txt
    val dataRdd: RDD[String] = sparkSession.sparkContext.textFile("/Users/tosin/Documents/IdeaProjects/spark-test/test-data/user.txt")

    //3. 切分数据
    val splitRdd: RDD[Array[String]] = dataRdd.map(_.split("\t"))

    //4. 封装数据
    val rowRdd: RDD[Row] = splitRdd.map(x => {
      val id: Int = x(0).toInt
      val name: String = x(1).toString
      val age: Int = x(2).toInt
      //封装一行数据
      Row(id, name, age)
    })

    //5. 创建schema(描述DataFrame信息) sql=表
    val schema: StructType = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
    //6. 创建DataFrame
    val userDF: DataFrame = sparkSession.createDataFrame(rowRdd,schema)

    //7. 注册表
    userDF.registerTempTable("user_t")

    //8. 写sql
    //val usql: DataFrame = sparkSession.sql("select * from user_t")
    val usql: DataFrame = sparkSession.sql("select * from user_t order by age")

    //9. 查看结果 show databases;
    usql.show()

    //10. 释放资源
    sparkSession.stop()
  }
}
