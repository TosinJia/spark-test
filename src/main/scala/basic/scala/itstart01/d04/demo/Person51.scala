package basic.scala.itstart01.d04.demo

/**
  * private[d04] 加包名，表示当前类和指定父包及父包的子包可见，可以访问
  * */
private[d04] class Person51(var name:String, age:Int) {
  var high:Int = _

  def this(name:String, age:Int, high:Int){
    this(name,age)
    this.high = high
  }
}
