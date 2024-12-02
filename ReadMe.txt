1. Run this code on one terminal to start DStream

while read line; do
  echo "$line"
  sleep 1
done < sentences.txt | nc -lk 9999 

2. Run this code on another terminal to start Spark Streaming

spark-submit k_mer.py
