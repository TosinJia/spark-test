package day0116

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现自定义排序
  * 按照年龄进行排序
  * 按照年龄进行排序,年龄相同体重大的往前排
  * */
object MySort1 {
  def main(args: Array[String]): Unit = {
    //1. spark程序入口
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    //2. 创建数组
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

    val sortedRdd: RDD[(String, Int, Int)] = grdd2.sortBy(t => {
      t._2
    }, false)
    val r: Array[(String, Int, Int)] = sortedRdd.collect()
    println(r.toBuffer)


    val grdd3: RDD[Girl] = grdd1.map(line => {
      val fields: Array[String] = line.split(",")
      //拿到每个属性
      val name: String = fields(0)
      val age: Int = fields(1).toInt
      val weight: Int = fields(2).toInt

      new Girl(name, age, weight)
    })
    val r2 = grdd3.sortBy(s => s).collect()
    println(r2.toBuffer)
    sc.stop()
  }
}

//自定义scala Ordered
class Girl(val name:String, val age:Int, val weight:Int) extends Ordered[Girl] with Serializable{
  override def compare(that: Girl): Int = {
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