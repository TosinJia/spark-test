package day0116

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MySort2 {
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
    //5. 模式匹配
    val sortedRdd: RDD[(String, Int, Int)] = grdd2.sortBy(s => {
      Girl2(s._1, s._2, s._3)
    })
    val r: Array[(String, Int, Int)] = sortedRdd.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

//样例类 支持模式匹配
case class Girl2(val name:String, val age:Int, val weight:Int) extends Ordered[Girl2]{
  override def compare(that: Girl2): Int = {
    //如果年龄相同，体重重的忘前排
    if(this.age == that.age){
      //正数正序
      -(this.weight-that.weight)
    }else{
      //年龄小的往前排
      this.age-that.age
    }
  }
  override def toString: String = s"name:$name,age:$age,weight:$weight"
}
