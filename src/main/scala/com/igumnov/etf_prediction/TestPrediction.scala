/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.igumnov.etf_prediction


// $example on$
import java.nio.file.{Paths, Files}

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import scala.io.Source

// $example off$
import org.apache.spark.sql.SparkSession

/**
  * An example for Multilayer Perceptron Classification.
  */
object TestPrediction {

  val SIZE = 3
  val SIZEVIX = 3
  var balanceGlobal = 0.0

  def main2(args: Array[String]): Unit = {

    import scala.io.Source
    val linesCsv = Source.fromFile("data/spy.csv").getLines()
    val linesCsvOrdered = linesCsv.toList.reverse

    val linesCsvVix = Source.fromFile("data/vix.csv").getLines()
    val linesCsvOrderedVix = linesCsvVix.toList.reverse

    val TRAIN = 200
    val TEST = 50



    val vixLearn = prepareLearnVix(linesCsvOrderedVix.drop(TEST).take(TRAIN))
    val vixTest = prepareLearnVix(linesCsvOrderedVix.take(TEST))

    val rates14learn1 = prepareLearnTest(linesCsvOrdered.drop(TEST).take(TRAIN))
    val rates14test1 = prepareLearnTest(linesCsvOrdered.take(TEST))

    val lines = prepareLines(rates14learn1, vixLearn)
    val lines2 = prepareLines(rates14test1, vixTest)


    Files.write(Paths.get("data/spy.txt"), lines.flatMap(s => (s + "\n").getBytes("utf8")).toArray)


    Files.write(Paths.get("data/spy1.txt"), lines2.flatMap(s => (s + "\n").getBytes("utf8")).toArray)

    iter(linesCsvOrdered.take(TEST), linesCsvOrderedVix.take(TEST))


  }

  def main(args: Array[String]): Unit = {

    import scala.io.Source

    val linesCsv = Source.fromFile("data/spy.csv").getLines()
    val linesCsvOrdered = linesCsv.toList.reverse

    val linesCsvVix = Source.fromFile("data/vix.csv").getLines()
    val linesCsvOrderedVix = linesCsvVix.toList.reverse

    val TRAIN = 200
    val TEST = 50

    for (i <- 0 until 1000) {
      import scala.io.Source


      val vixLearn = prepareLearnVix(linesCsvOrderedVix.drop(i * 25).drop(TEST).take(TRAIN))
      val vixTest = prepareLearnVix(linesCsvOrderedVix.drop(i * 25).take(TEST))

      val rates14learn1 = prepareLearnTest(linesCsvOrdered.drop(i * 25).drop(TEST).take(TRAIN))
      val rates14test1 = prepareLearnTest(linesCsvOrdered.drop(i * 25).take(TEST))

      val lines = prepareLines(rates14learn1, vixLearn)
      val lines2 = prepareLines(rates14test1, vixTest)


      Files.write(Paths.get("data/spy.txt"), lines.flatMap(s => (s + "\n").getBytes("utf8")).toArray)


      Files.write(Paths.get("data/spy1.txt"), lines2.flatMap(s => (s + "\n").getBytes("utf8")).toArray)

      iter(linesCsvOrdered.drop(i * 25).take(TEST), linesCsvOrderedVix.drop(i * 25).take(TEST))

    }

  }

  def iter(linesCsvOrdered: List[String], linesCsvOrderedVix: List[String]) = {

    val spark = SparkSession
      .builder
      .appName("TestPrediction")
      .getOrCreate()

    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    val dataTrain = spark.read.format("libsvm")
      .load("data/spy.txt")
    val dataTest = spark.read.format("libsvm")
      .load("data/spy1.txt")
    // Split the data into train and test
    //    val splitsTrain = dataTrain.randomSplit(Array(0.99, 0.01), seed = 1234L)
    //    val splitsTest = dataTest.randomSplit(Array(0.99, 0.01), seed = 1234L)
    val train = dataTrain
    val test = dataTest
    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    //    val layers = Array[Int](SIZE-1, SIZE*3 ,SIZE, 2)
    val layers = Array[Int](SIZE + SIZEVIX, (SIZE + SIZEVIX) * 8, SIZE, 3)
    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)
    // train the model
    val model = trainer.fit(train)
    // compute accuracy on the test set
    val result = model.transform(test)
    var resultStr: scala.collection.mutable.ListBuffer[String] = new scala.collection.mutable.ListBuffer[String]()
    result.collect().foreach(r => {
      resultStr += toStr(r.getAs("prediction"))
    }
    )
    val lines = (resultStr.reverse, linesCsvOrdered.reverse.drop(1), linesCsvOrderedVix.reverse.drop(1)).zipped.toList
      .map(x => {
        val x3 = x._3.split(" ").dropRight(1).takeRight(1).mkString(" ")
        val ret = x._1 + " " + x._2 + " " + x3
        println(ret)
        ret
      })

    Files.write(Paths.get("data/out.csv"), lines.flatMap(s => (s + "\n").getBytes("utf8")).toArray)
    calcProfit()
    val predictionAndLabels = result.filter(r => {
      (r.getAs("prediction") == 2.0) || (r.getAs("prediction") == 0.0)
    }).select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    println("Accuracy: " + evaluator.evaluate(predictionAndLabels))
    // $example off$

    spark.stop()
  }


  def toStr(in: Double) = {
    (in - 1).toString
  }

  def normal3(set: List[Double]) = {
    val begin = set.head
    set.map(rate => {
      begin - rate
    })

  }


  def tail17(lines: List[String]): List[List[String]] = {
    if (lines.size >= SIZE + 4)
      List(lines.take(SIZE + 4)) ++ tail17(lines.tail)
    else
      List(lines.take(SIZE + 4))
  }


  def prepareLearnVix(linesCsvOrdered: List[String]) = {
    val lines17 = tail17(linesCsvOrdered)
    val rates17 = lines17.map(set =>
      set.map(line =>
        (line.split(" ").takeRight(2).take(1)).apply(0).toDouble
      )
    )
    val rates14learn = rates17.map(set => {
      set.take(SIZEVIX)
    })
    rates14learn
  }


  def prepareLines(rates14learn: List[List[AnyVal]], vixlearn: List[List[AnyVal]]) = {
    val zip = rates14learn.zip(vixlearn)
    zip.map(zzz => {
      val set = zzz._1 ++ normal3(zzz._2.take(SIZEVIX).asInstanceOf[List[Double]])
      val a = set.tail.map(":" + _)
      val b = a.zip((1 to SIZE + SIZEVIX).toList)
      val c = b.map(x => {
        (x._2.toString) + x._1
      })
      set(0) + " " + c.mkString(" ")
    }
    )
  }


  def prepareLearnTest(linesCsvOrdered: List[String]) = {
    val lines17 = tail17(linesCsvOrdered)
    val rates17 = lines17.map(set =>
      set.map(line =>
        (line.split(" ").apply(1)).toDouble
      )
    )

    val rates14learn = rates17.map(set => {


      val last4 = set.takeRight(5)

      var teach = 0
      if ((last4.head < last4.tail(0))) {
        teach = 1

      } else {
        if ((last4.head > last4.tail(0))) {
          teach = -1
        }
      }
      teach = teach + 1

      List(teach) ++ normal3(set.take(SIZE))
    })
    rates14learn
  }

  def calcProfit() = {
    var balance = 0.0
    val linesCsv = Source.fromFile("data/out.csv").getLines()
    val linesCsvOrdered = linesCsv.toList.reverse
    val tailLine = tailbalance(linesCsvOrdered)
    tailLine.map(set => {
      set.map(line => {
        val price = line.split(" ").apply(0)
        val what = line.split(" ").apply(2)
        val vix = line.split(" ").apply(3)
        (what, price, vix)
      })
    }).foreach(line => {
      if (line.size == 3) {
        val price0 = line(0)._1.toDouble
        val price1 = line(1)._1.toDouble
        val what = line(0)._2
        val vix = line(0)._3.toDouble
//        if (what == "-1.0" && (vix > 30.0)) {
//          balance = balance + price0 - price1
//        }
        if (what == "1.0" && (vix < 10.0)) {
          balance = balance + price1 - price0
        }
        println("Accuracy balance: " + balance)


      }
    })
    balanceGlobal = balanceGlobal + balance
    //    println("Accuracy balance: " + balance)
    println("Accuracy global balance: " + balanceGlobal)

  }

  def tailbalance(lines: List[String]): List[List[String]] = {
    if (lines.size >= 3)
      List(lines.take(3)) ++ tailbalance(lines.tail)
    else
      List(lines.take(3))
  }

}

// scalastyle:on println
