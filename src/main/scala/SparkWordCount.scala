import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark-WordCount本地模式测试
  * java.io.FileNotFoundException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset. -see https://wiki.apache.org/hadoop/WindowsProblems
  *  配置环境变量
  *     HADOOP_HOME E:\tools\hadoop-2.8.4
  *     PATH %HADOOP_HOME%\bin;%HADOOP_HOME%\sbin
  * */
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    //2. 设置参数 setAppNmae设置程序名 setMaster本地测试设置线程数 * 多个
    val conf:SparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    //1. 创建spark执行程序的入口
    val sc:SparkContext = new SparkContext(conf)
    //3. 加载数据并处理
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .saveAsTextFile(args(1))
    //4. 关闭资源
    sc.stop()
  }
}
