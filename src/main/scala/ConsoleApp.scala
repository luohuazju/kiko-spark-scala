package com.kiko.spark.trips

object ConsoleApp extends App {

  def printDown(n:Int): Unit = {
    n match {
      case n if n >= 0 => (0 to n reverse) foreach println
      case n if n < 0 => n to 0 foreach println
    }
  }
  println("--------------")
  printDown(2)
  printDown(-2)
  println("--------------")
}

