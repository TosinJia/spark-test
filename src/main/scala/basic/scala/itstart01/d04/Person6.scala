package basic.scala.itstart01.d04

class Person6(private var name:String, age:Int) {
  var high:Int = _

  def this(name:String, age:Int, high:Int){
    this(name, age)
    this.high = high
  }
}

//注意：在伴生对象中可以访问类的私有成员方法和属性
//什么是伴生对象？单例类名与类名相同
object Person6 extends App{
//object Person5 extends App{  //private var name:String 不可访问
  val p6 = new Person6("tosin",age=18)
  println(p6.name)
  println(p6.high)
}
