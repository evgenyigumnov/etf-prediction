package com.igumnov.etf_prediction

/**
  * Created by secret on 15/09/16.
  */
object CalcProfit {

  import scala.io.Source


  def main(args: Array[String]): Unit = {

    var balance = 0.0
    val linesCsv = Source.fromFile("data/out.csv").getLines()
    val linesCsvOrdered = linesCsv.toList.reverse
    val tailLine = tailbalance(linesCsvOrdered)
    tailLine.map(set => {
      set.map(line => {
        val price = line.split(" ").apply(0)
        val what = line.split(" ").apply(2)
        (what, price)
      })
    }).foreach(line => {
      if (line.size == 3) {
        println(line)
        println(balance)
        val price0 = line(0)._1.toDouble
        val price1 = line(1)._1.toDouble
        val price2 = line(2)._1.toDouble
        val what = line(0)._2
        if (what == "-1.0") {
          if (price0 > price1) {
            balance = balance + price0 - price1
          } else {
            balance = balance + price0 - price2
          }
        } else {
          if (what == "1.0") {
            if (price0 < price1) {
              balance = balance + price1 - price0
            } else {
              balance = balance + price2 - price0
            }
          }
        }

      }
    })


  }

  def tailbalance(lines: List[String]): List[List[String]] = {
    if (lines.size >= 3)
      List(lines.take(3)) ++ tailbalance(lines.tail)
    else
      List(lines.take(3))
  }
}
