package day200117

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * SparkGraphX
 * https://mvnrepository.com/artifact/org.apache.spark/spark-graphx_2.11/2.2.0
 * 1~02-03 GraphX讲义.docx 2.4.3操作一览
 */
object Demo2 {
  def main(args: Array[String]): Unit = {
    //获取spark的环境
    val conf = new SparkConf().setAppName("GraphX").setMaster("local")
    val sc = new SparkContext(conf)
    //创建顶点
    val users = sc.parallelize(Array((3L,("rxin","stu")),(5L, ("franklin", "prof")),(7L, ("jgonzal", "pst.doc")),(2L, ("istoica", "prof"))))

    //创建边
    val rdge = sc.parallelize(Array(Edge(3L,7L, "Collab"),
      Edge(5L,3L,"Adcisor"), Edge(5L,7L,"PI"), Edge(2L,5L, "Colleague")))
    //构建图
    val graph = Graph(users, rdge)
    //查询
    //查看图中有多少个prof
    val prof_count = graph.vertices.filter{case (id, (name, pos)) => pos=="prof"}.count()

    println(prof_count)

    sc.stop()
  }
}
