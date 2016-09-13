#!/bin/bash

# Author: Wenyi Xu
# Copyright (c) 2016 WenyiXu

python src/stream.py &
$SPARK_HOME/bin/spark-submit src/sparkjob.py &
