package day200117

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
 * 机器学习
 * 线性回归
 * [root@bd-01-01 spark-2.4.4-bin-hadoop2.7]# ./bin/run-example mllib.LinearRegression ./data/mllib/sample_linear_regression_data.txt
 * https://mvnrepository.com/artifact/org.apache.spark/spark-mllib_2.11/2.2.0
 */
object Demo1 {
  def main(args: Array[String]): Unit = {

    //获取spark session
    val sparkSession: SparkSession = SparkSession.builder().appName("mllib").master("local").getOrCreate()

    //读取数据
    val trainning = sparkSession.read.format("libsvm").load("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\spark-test\\data\\SparkMLlib\\sample_linear_regression_data2.txt")

    //声明一个模型
    val lr = new LinearRegression().setMaxIter(10000)
    //训练模型
    val lrMode = lr.fit(trainning)
    //查看训练结果
    val trainningSummary = lrMode.summary

    //查看预测结果
    trainningSummary.predictions.show()
    //查看误差
    println("RMSE = "+trainningSummary.rootMeanSquaredError)

    sparkSession.stop()
  }
}
