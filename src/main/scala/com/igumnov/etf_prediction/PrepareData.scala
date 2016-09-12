package com.igumnov.etf_prediction

import java.nio.file.{Paths, Files}


//Акции компаний развитых стран:
//США – 25% SPY
//ЕВРОПА – 15% VGK
//
//Акции компаний развивающихся стран:
//БРИК – 10% VWO
//
//Золото - 10% GLD
//
//Облигации казначейские развитых стран краткосрочные 1-3 – 30% SHY
//Облигации казначейские развитых стран долгосрочные 10-20 – 10% BLV


object PrepareData {

  def main(args: Array[String]): Unit = {
    import scala.io.Source

    val linesCsv = Source.fromFile("data/spy.csv").getLines()
    val linesCsvOrdered = linesCsv.toList.reverse
    val lines17 = tail17(linesCsvOrdered)
    val rates17 = lines17.map(set =>
      set.map(line =>
        (line.split(" ").apply(1)).toDouble
      )
    )
    val rates17normal = rates17.map(normal(_))

    val rates14learn = rates17normal.map(set => {
      val last4 =  set.takeRight(3)
      val teach = if (last4.head < last4.last) {
        if (last4.head < last4.apply(1) &&
          last4.apply(1) < last4.apply(2)) 1
        else 0
      } else {
        if (last4.head > last4.last) {
          if (last4.head > last4.apply(1) &&
            last4.apply(1) > last4.apply(2) ) 2
          else 0
        } else {
          0
        }
      }
      List(teach) ++   set.take(5)
    })


    val lines = rates14learn.map(set => {
      val a = set.takeRight(5).map(":" + _)
      val b = a.zip((1 to 5).toList)
      val c = b.map(x=> {
         (x._2.toString)+x._1
      })
      set(0) + " " + c.mkString(" ")
    }
    )

    Files.write(Paths.get("data/spy.txt"), lines.flatMap(s => (s + "\n").getBytes("utf8")).toArray)

  }

  def normal(set: List[Double]) = {
    val max = set.max
    val min = set.min
    val middle = ((max - min) / 2.0) + min
    set.map(rate => {
      (((((max - rate) / (max - min) * (-1)) + 1) * 2) - 1)
    })

  }

  def tail17(lines: List[String]): List[List[String]] = {
    if (lines.size > 5 + 2)
      List(lines.take(5 + 2)) ++ tail17(lines.tail)
    else
      List(lines.take(5 + 2))
  }

}
