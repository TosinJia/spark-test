package day200106

import org.apache.spark.sql.SparkSession

/**
 * 2、使用case class
 * spark session方式使用spark sql
 * 使用case class方式操作
 */
object Demo2 {
  def main(args: Array[String]): Unit = {
    //创建spark session
    val spark = SparkSession.builder().appName("CaseClassDemo").master("local").getOrCreate()

    //获取数据RDD
    val fileRDD = spark.sparkContext.textFile("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\spark-test\\data\\SparkSql\\student.txt").map(_.split("\t"))
    ///将数据映射到caseclass中
    val studentRDD = fileRDD.map(x => Student(x(0).toInt, x(1), x(2).toInt))
    //创建DF,注意需要导包
    import spark.sqlContext.implicits._
    val studentDF = studentRDD.toDF
    //注册视图
    studentDF.createOrReplaceTempView("student")
    //执行sql
    spark.sql("select * from student").show()

    spark.stop()
  }

  case class Student(stuId:Int, stuName:String, stuAge:Int)
}
