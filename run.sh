#!/bin/bash
cd /Users/secret/etf-prediction
sbt clean package
cp -f target/scala-2.11/simple-project_2.11-1.0.jar /Users/secret/spark/bin/
cd /Users/secret/spark/bin/
./spark-submit --class "com.igumnov.etf_prediction.TestPrediction" simple-project_2.11-1.0.jar 2>1 |grep -v INFO