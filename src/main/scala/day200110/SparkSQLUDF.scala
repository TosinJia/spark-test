package day200110

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 实现UDF
 */
object SparkSQLUDF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //获取spark-sql的环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("UDF")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    //不建议使用，一般使用Spark Session
    val sqlContext = new SQLContext(sparkContext)
    //获取数据：Array
    val data:Array[String] = Array("spark", "spark", "hadoop", "hadoop", "hadoop", "java", "scala", "scala")
    //转换Datafrema,是将RDD转换为DF
    val dataRDD = sparkContext.parallelize(data)
    //将数据映射到Row
    val dataRow = dataRDD.map(x => Row(x))
    //创建表结构
    val structType = StructType(
      Array(
        new StructField("name", StringType)
      )
    )
    val dataDF = sqlContext.createDataFrame(dataRow, structType)
    //注册成表
    dataDF.createOrReplaceTempView("word")
    //实现一个自定以函数:将每个name的长度打印出来
    //第一个参数：定义udf的名字；第二个参数：定义udf具体的实现;spark-sql的udf最多支持22个列
    sqlContext.udf.register("myUDF", (x:String)=> x.length)
    //写sql，使用自定义函数UDF
    sqlContext.sql("select * from word").show
    sqlContext.sql("select name, myUDF(name) as length from word").show

    //定义一个udad聚合函数
    sqlContext.udf.register("myUDAD", new MyUDAF)
    sqlContext.sql("select name, myUDAD(name) as count from word group by name").show

  }
}

class MyUDAF extends UserDefinedAggregateFunction{
  //指定输入数据类型
  override def inputSchema: StructType = {
    StructType(Array(
      StructField("input", StringType)
    ))
  }

  //聚合操作时，临时存储一些数据
  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("count", IntegerType)
    ))
  }

  //指定UDAF计算返回结果类型
  override def dataType: DataType = {IntegerType}

  //确保一致性，一般设置true
  override def deterministic: Boolean = true

  //初始buffer，存储一些临时结果
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //在聚合时，每次会有新的值进来，对分组之后的值进行计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0)+1
  }

  //最后分布式计算reduce归约，reduce结束之后进行全局merge合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
  }

  //返回计算的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
