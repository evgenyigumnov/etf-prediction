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

// $example off$
import org.apache.spark.sql.SparkSession

/**
  * An example for Multilayer Perceptron Classification.
  */
object TestPrediction {

  val SIZE = 10

  def main(args: Array[String]): Unit = {

    import scala.io.Source

    val linesCsv = Source.fromFile("data/spy.csv").getLines()
    val linesCsvOrdered = linesCsv.toList.reverse
//
//    val TRAIN = 1700
//    val rates14learn1 = prepareLearn(linesCsvOrdered.dropRight(TRAIN))
//    val rates14test1 = prepareLearn(linesCsvOrdered.takeRight(TRAIN))
    val TRAIN = 1700
    val rates14learn1 = prepareLearn(linesCsvOrdered.take(1500))
    val rates14test1 = prepareLearn(linesCsvOrdered.takeRight(600))

    val lines = prepareLines(rates14learn1)
    val lines2 = prepareLines(rates14test1)


    Files.write(Paths.get("data/spy.txt"), lines.flatMap(s => (s + "\n").getBytes("utf8")).toArray)


    Files.write(Paths.get("data/spy1.txt"), lines2.flatMap(s => (s + "\n").getBytes("utf8")).toArray)

    Files.write(Paths.get("data/spy2.txt"), tail17(linesCsvOrdered.takeRight(TRAIN)).flatMap(s => (s + "\n").getBytes("utf8")).toArray)



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
    val layers = Array[Int](SIZE, SIZE*8 ,SIZE, 2)
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
      println("prediction:" + toStr(r.getAs("prediction")))
      resultStr += toStr(r.getAs("prediction"))
    }
    )
    resultStr.reverse.zip(linesCsvOrdered.reverse.drop(1)).foreach(x => {
      println(x._1 + " " + x._2)
    })
    val predictionAndLabels = result.filter(r => {
      true //r.getAs("prediction") == 1.0
    }).select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    println("Accuracy: " + evaluator.evaluate(predictionAndLabels))
    // $example off$

    spark.stop()
  }


  def toStr(in: Double) = {
    if (in == 0.0) "Sell" else "Buy"
  }

  def normal(set: List[Double]) = {
    val max = set.max
    val min = set.min
    val middle = ((max - min) / 2.0) + min
    set.map(rate => {
      (((((max - rate) / (max - min) * (-1)) + 1) * 2) - 1)
    })

  }

  def normal2(set: List[Double]) = {
    set.zip(set.tail).map(x => {
      if (x._1 > x._2) 1 else -1
    }).tail
  }

  def tail17(lines: List[String]): List[List[String]] = {
    if (lines.size > SIZE + 3)
      List(lines.take(SIZE + 3)) ++ tail17(lines.tail)
    else
      List(lines.take(SIZE + 3))
  }

  def prepareLines(rates14learn: List[List[AnyVal]]) = {
    rates14learn.map(set => {
      //println(set)
      val a = set.tail.map(":" + _)
      val b = a.zip((1 to SIZE).toList)
      val c = b.map(x => {
        (x._2.toString) + x._1
      })
      set(0) + " " + c.mkString(" ")
    }
    )
  }

  def prepareLearn(linesCsvOrdered: List[String]) = {
    val lines17 = tail17(linesCsvOrdered)
    val rates17 = lines17.map(set =>
      set.map(line =>
        (line.split(" ").apply(1)).toDouble
      )
    )

    val rates14learn = rates17.map(set => {
      val last4 = normal(set).takeRight(2)
      val teach = if ((last4(0) < last4(1))) {
        1
//        if ((last4(1) - last4(0)) > 0.05) 1 else 0
      } else 0
      List(teach) ++ normal2(set).take(SIZE)
    })
    rates14learn
  }

}

// scalastyle:on println
