#!/bin/bash

while read line; do
  echo "$line"
  sleep 1
done < sentences.txt | nc -lk 9999
