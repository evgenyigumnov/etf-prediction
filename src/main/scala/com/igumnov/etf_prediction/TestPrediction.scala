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

  val SIZE = 8

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
    //    val rates17normal = rates17.map(normal(_))

    val rates14learn = rates17.map(set => {
      val last4 = normal(set).takeRight(3)
      val teach = if ((last4(0) < last4(1)) && (last4(1) < last4(2))) {
        1
      }
      else 0
      List(teach) ++ normal(set.take(SIZE))
    })


    //    val lines = prepareLines(rates14learn.dropRight(1000))
    val lines = prepareLines(rates14learn.dropRight(500))

    Files.write(Paths.get("data/spy.txt"), lines.flatMap(s => (s + "\n").getBytes("utf8")).toArray)

    //    val lines2 = prepareLines(List(rates14learn.last))
    val lines2 = prepareLines(rates14learn.takeRight(500))

    Files.write(Paths.get("data/spy1.txt"), lines2.flatMap(s => (s + "\n").getBytes("utf8")).toArray)



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
    val splitsTrain = dataTrain.randomSplit(Array(0.99, 0.01), seed = 1234L)
    val splitsTest = dataTrain.randomSplit(Array(0.99, 0.01), seed = 1234L)
    val train = splitsTrain(0)
    val test = splitsTest(0)
    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](SIZE, SIZE+2 , 3, 2)
    //val layers = Array[Int](SIZE, SIZE , 5, 2)
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
    result.foreach(r => {
      println("prediction:" + toStr(r.getAs("prediction")))
    }
    )
    val predictionAndLabels = result.filter(r => {
      r.getAs("prediction") == 1.0
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

  def tail17(lines: List[String]): List[List[String]] = {
    if (lines.size > SIZE + 2)
      List(lines.take(SIZE + 2)) ++ tail17(lines.tail)
    else
      List(lines.take(SIZE + 2))
  }

  def prepareLines(rates14learn: List[List[AnyVal]]) = {
    rates14learn.map(set => {
      val a = set.takeRight(SIZE).map(":" + _)
      val b = a.zip((1 to SIZE).toList)
      val c = b.map(x => {
        (x._2.toString) + x._1
      })
      set(0) + " " + c.mkString(" ")
    }
    )
  }

}

// scalastyle:on println
