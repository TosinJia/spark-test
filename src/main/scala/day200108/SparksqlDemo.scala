package day200108

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import day200108._
import org.apache.log4j.{Level, Logger}

object SparksqlDemo {
  def main(args: Array[String]): Unit = {

    //去除无用INFO
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //获取sparksession
    val spark: SparkSession = SparkSession.builder().master("local").appName("SparksqlDemo").getOrCreate()

    //创建学生表
    import spark.sqlContext.implicits._
    spark.sparkContext.textFile("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\spark-test\\data\\SparkSql\\Student.csv").map(_.split(","))
      .map(x => Student(x(0), x(1), x(2), x(3), x(4)))
      .toDF
      .createOrReplaceTempView("student")
//    spark.sql("select * from student").show

    //创建成绩表
    spark.sparkContext.textFile("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\spark-test\\data\\SparkSql\\Score.csv").map(_.split(","))
      .map(x => Score(x(0), x(1), x(2)))
      .toDF
      .createOrReplaceTempView("score")
//    spark.sql("select * from score").show

    //创建课程表
    spark.sparkContext.textFile("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\spark-test\\data\\SparkSql\\Course.csv").map(_.split(","))
      .map(x => Course(x(0), x(1), x(2)))
      .toDF
      .createOrReplaceTempView("course")
//    spark.sql("select * from course").show

    //创建教师表
    spark.sparkContext.textFile("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\spark-test\\data\\SparkSql\\Teacher.csv").map(_.split(","))
      .map(x => Teacher(x(0), x(1), x(2), x(3), x(4), x(5)))
      .toDF
      .createOrReplaceTempView("teacher")
    //不省略字段信息
//    spark.sql("select * from teacher").show(false)

    //查询学生的姓名、性别、生日
//    spark.sql("select sname, ssex, sbirthday from student").show

    //查询教师表不重复的部门
//    spark.sql("select distinct tdepartment from teacher").show(false)
//    spark.sql("select tdepartment, count(1) from teacher group by tdepartment").show(false)

    //查询成绩表中所有在60分到80之间的(包含60和80)
//    spark.sql("select * from score where degree>=60 and degree<=80").show
//    spark.sql("select * from score where degree between 60 and 80").show
    //查询成绩表中成绩为：88，92，81的数据
//    spark.sql("select * from score where degree='88' or degree='81' or degree='92'").show
//    spark.sql("select * from score where degree in(88,81,92)").show

    //查询score表的degree，降序
//    spark.sql("select * from score order by degree desc").show
    //注意：排序使用String类型按照字典顺序排序,需要进行类型转换
//    spark.sql("select * from score order by int(degree) desc").show

    //查询没门课程的平均成绩;spark-sql对于大小写也是不敏感的 avg(CAST(degree AS DOUBLE))
//    spark.sql("select cnum, avg(degree) from score group by cnum").show
//    spark.sql("SELECT CNUM, AVG(CAST(DEGREE AS INT)) FROM SCORE GROUP BY CNUM").show

    //查询score中至少有5名学生选课，并且课程编号是3开头的课程的平均分
//    spark.sql("select cnum, count(1), avg(degree) from score where cnum like '3%' group by cnum having count(1)>=2").show

    //查询score表中选择多门课程的同学，最高分的学生
//    spark.sql("select snum, degree from score " +
//      "where snum in(select t.snum from score t group by t.snum having count(t.snum)>1) " +
//      "and degree = (select max(degree) from score)").show
    //查询学生的姓名和成绩以及课程名
//    spark.sql("select sname, cname, degree from score t1 join student s on t1.snum=s.snum " +
//      "join course c on t1.cnum=c.cnum ").show(false)

    //查询学生的学号为108的同年的所有学生
//    spark.sql("select * from student where substring(sbirthday, 0, 4) = (select substring(sbirthday, 0, 4) from student where snum=108)").show

    //查询选修课程人数大于5，的课程的教师的姓名
//    spark.sql("select tname from teacher t join course c on t.tnum=c.tnum " +
//      "join(select cnum from score group by cnum having count(1)>5) temp on c.cnum=temp.cnum").show

    //查询成绩比改成成绩低的学生成绩  里边使用外边表的字段--??
    spark.sql("select * from score s where s.degree<(select avg(degree) from score c where s.snum=c.snum)").show
    //查询所有没有讲课的教师
//    spark.sql("select * from teacher t where t.tnum not in(" +
//      "select tnum from course where cnum in(select distinct cnum from score))").show

    //查询学生的年龄
//    spark.sql("select sname, (CAST("+getDate("yyyy")+" as int)-CAST(substring(sbirthday,0,4) as int)) sage,  CAST("+getDate("yyyy")+" as int), CAST(substring(sbirthday,0,4) as int) from student").show
    spark.stop()
  }

  //获取当前年份
  def getDate(pattern:String)={
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat(pattern)
    simpleDateFormat.format(new Date())
  }
}
