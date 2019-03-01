package basic.scala.itstart01.d02

import java.util.Date

object PartParamApp extends App {
  def log(date: Date, message: String): Unit = {
    println(s"$date, $message")
  }

  val date = new Date();
  val logMessage = log(date, _:String);

  log(date, "tosin test1");
  logMessage("tosin test2");
}
