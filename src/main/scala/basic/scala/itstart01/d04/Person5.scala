package basic.scala.itstart01.d04

/**
  * private[this] private 类的访问权限，[this]表示当前类和当前包及其子包可见，可以访问
  * private [this]默认
  * */
private class Person5(var name:String, age:Int) {
//private[this] class Person5(var name:String, age:Int) {
  var high:Int = _

  def this(name:String, age:Int, high:Int){
    this(name,age)
    this.high = high
  }
}
