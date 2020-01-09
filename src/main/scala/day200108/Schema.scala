package day200108


/**
 * 学生表
 * @param snum 编号
 * @param sname 姓名
 * @param ssex 性别
 * @param sbirthday 生日
 * @param sclass 班级
 */
case class Student(snum:String, sname:String, ssex:String, sbirthday:String, sclass:String) {}

/**
 * 成绩表
 * @param snum 学号
 * @param cnum 课程号
 * @param degree 成绩
 */
case class Score(snum:String, cnum:String, degree:String){}

/**
 * 课程表
 * @param cnum 课程编号
 * @param cname 课程名称
 * @param tnum 教师编号
 */
case class Course(cnum:String, cname:String, tnum:String){}

/**
 * 教师表
 * @param tnum 教师编号
 * @param tname 教师名称
 * @param tsex 教师性别
 * @param tbirthday 教师生日
 * @param tprof 职称
 * @param tdepartment 教师所在部门
 */
case class Teacher(tnum:String, tname:String, tsex:String, tbirthday:String, tprof:String, tdepartment:String){}