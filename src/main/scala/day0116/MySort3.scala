package day0116

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 建议用
  * 隐式转换 定义一个专门处理隐式的类
  * */
object ImplicitRules {
  //定义隐式规则
  implicit object OrderingGirl extends Ordering[Girl3]{
    override def compare(x: Girl3, y: Girl3): Int = {
      if(x.age == y.age){
        //体重重的往前排
        -(x.weight-y.weight)
      }else{
        //年龄小的往前排
        x.age-y.age
      }
    }
  }
}

object MySort3{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val girls: Array[String] = Array("reba,18,80","mimi,22,100","liya,30,100","jingtian,18,78")
    //3. 并行化，转化RDD
    val grdd1: RDD[String] = sc.parallelize(girls)

    //4. 切分数据
    val grdd2: RDD[(String, Int, Int)] = grdd1.map(line => {
      val fields: Array[String] = line.split(",")
      //拿到每个属性
      val name: String = fields(0)
      val age: Int = fields(1).toInt
      val weight: Int = fields(2).toInt

      //元组输出
      (name, age, weight)
    })

    import ImplicitRules.OrderingGirl

    val sortedRdd: RDD[(String, Int, Int)] = grdd2.sortBy(s => {
      Girl3(s._1, s._2, s._3)
    })
    val r: Array[(String, Int, Int)] = sortedRdd.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

case class Girl3(val name:String, val age:Int, val weight:Int)