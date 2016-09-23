#!/bin/bash

# Author: Wenyi Xu
# Copyright (c) 2016 WenyiXu

# Run the Twitter streamer
python3.4 src/stream.py
# Run the MongoDB server
sudo mongod
# Run the analysis
$SPARK_HOME/bin/spark-submit src/analysis.py > log/analysis.log
# Run the data visualization
python3.4 web/dashboard.py
