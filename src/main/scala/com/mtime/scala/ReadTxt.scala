package com.mtime.scala

import scala.io.Source

object ReadTxt {
  def main(args: Array[String]): Unit = {
    //var dataList = List(11,22,33,44)
    //dataList.flatMap(s=>s.toString)
    var dataList = List(List(11,22,33),List(33,44),List(55,66))
    println(dataList)
    //dataList.foreach(println)
    dataList.flatMap(_.toIterable).foreach(println)
  }
}
