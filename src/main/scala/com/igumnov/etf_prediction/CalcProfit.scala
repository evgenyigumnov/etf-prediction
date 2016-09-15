
package com.igumnov.etf_prediction

object CalcProfit {

  import scala.io.Source


  def main(args: Array[String]): Unit = {

    val linesCsv = Source.fromFile("data/spy.csv").getLines()
    val linesCsvOrdered = linesCsv.toList.reverse
    val linesCsvVix = Source.fromFile("data/vix-new.csv").getLines()
    val linesCsvOrderedVix = linesCsvVix.toList.reverse

    val spy = linesCsvOrdered.map(line =>
      line.split(" ").takeRight(1)(0).toDouble
    )
    //    println(spy)
    val vix = linesCsvOrderedVix.map(line =>
      line.split(" ").takeRight(1)(0).toDouble
    )
    //    println(vix)
    val all = spy.zip(vix)
    //    println(all)
    val good = all.map(set => {
      val s = set._1
      val v = set._2
      if (v > 20.0) (s, 0.0) else (0.0, s)
    })
    val bad = all.map(set => {
      set._1
    })
    //    println(good.filter(_._1 == 0).size)
    //    println(bad.filter(_._1 != 0) size)
    println(good)
    val goodProfit = good.foldLeft(0.0,0.0)((x: Any, y: Any) => {
      val xx = x.asInstanceOf[(Double, Double)]
      val yy = y.asInstanceOf[(Double, Double)]
      ((xx._1 + (spy.last -yy._1 )),0.0)
    })
    println(goodProfit)
    val goodSafe = good.foldLeft(0.0,0.0)((x: Any, y: Any) => {
      val xx = x.asInstanceOf[(Double, Double)]
      val yy = y.asInstanceOf[(Double, Double)]
      (0.0,((xx._2) + (yy._2)))
    })
    println(goodSafe)
    println(goodProfit._1+goodSafe._2)
    val badProfit = bad.foldLeft(0.0)(_ + spy.last - _)
    println(badProfit)

  }
}
