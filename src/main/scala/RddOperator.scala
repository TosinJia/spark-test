import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RddOperator").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // |
    // "|" "345" => "1" "" => "0"
    // "|" "12" => "2" "23" > "1"
    val rdd1: RDD[String] = sc.parallelize(List("12","23","345",""),2)
    val str11: String = rdd1.aggregate("|")(_+_,_+_)
    val str12: String = rdd1.aggregate("|")((x,y)=>math.min(x.length,y.length).toString, (x,y)=>x+y)
    println(str11+"\t"+str12)

    // |
    // "|" "" => "0" "456" => "1"
    // "|" "12" => "1" "23" => "1"
    val rdd2: RDD[String] = sc.parallelize(List("12","23","","456"),2)
    val str21: String = rdd2.aggregate("|")(_+_,_+_)
    val str22: String = rdd2.aggregate("|")((x,y)=>math.min(x.length,y.length).toString, (x,y)=>x+y)
    println(str11+"\t"+str12+"\n"+str21+"\t"+str22)

    sc.stop()
  }
}
