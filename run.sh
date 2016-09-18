#!/bin/bash

# Author: Wenyi Xu
# Copyright (c) 2016 WenyiXu

python src/stream.py > log/stream.log &
$SPARK_HOME/bin/spark-submit src/analysis.py > log/sparkjob.log 
