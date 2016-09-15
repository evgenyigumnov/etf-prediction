#!/bin/bash
cd data
mkdir in
cd in
for i in {1..15}
do
   echo "Welcome $i times"
   let j="(i-1)*200"
   echo $i $j
   wget https://www.google.com/finance/historical?cid=87692982100134\&startdate=Sep%2011%2C%202005\&enddate=Sep%209%202016\&num=200\&ei=fBXaV6i2L4i2swHXrYaQAg\&start=$j --output-document=$i.html
   html2text $i.html > $i.txt
   cat $i.txt |grep 20| sed '1,2d' >$i.csv
done
for i in {1..15}
do
    cat $i.csv >>../vix.csv
done

cd ..
cd ..
