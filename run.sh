#!/bin/bash

# Author: Wenyi Xu
# Copyright (c) 2016 WenyiXu

python stream.py
$SPARK_HOME/spark-submit sparkjob.py
