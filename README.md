![Python 3.4](https://img.shields.io/badge/python-3.4-green.svg)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)]()
# Twitter Hashtag Tracking

## Motivation
Track specific hashtags or keywords in Twitter, do real-time analysis on the tweets.

## Run Example
### Configuration
Modify the conf/parameters.json file to set the parameters.
```json
{ "hashtag": "#overwatch",
  "DStream": { "batch_interval": "10",
               "window_time": "10",
               "process_times": "10" }
}
```
### MongoDB Database
Start a mongod process
```
sudo mongod
```
### Twitter Input
Wait for connection to start streaming tweets.
```
python3.4 src/stream.py
```
### Spark Streaming
Run Spark jobs to do real-time analysis on the tweets.
```
$SPARK_HOME/bin/spark-submit src/analysis.py > log/analysis.log
```
### Dashboard
Run the data visualization jobs.
```
python3.4 web/dashboard.py
```

## Process

### Visualization
Time line of related tweet counts.
<p align="justify">
  <img src="https://github.com/xuwenyihust/Twitter-Hashtag-Tracking/blob/master/img/timeline.JPG" width="200"/>
  <img src="https://github.com/xuwenyihust/Twitter-Hashtag-Tracking/blob/master/img/hashtags.JPG" width="200"/>
  <img src="https://github.com/xuwenyihust/Twitter-Hashtag-Tracking/blob/master/img/keywords.JPG" width="200"/>
  <img src="https://github.com/xuwenyihust/Twitter-Hashtag-Tracking/blob/master/img/Ratio.JPG" width="150"/>
</p>

## Prerequisite
* [pyspark](http://spark.apache.org/docs/latest/api/python/pyspark.html)
* [pyspark.sql](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=udf)
* [pyspark.streaming](https://spark.apache.org/docs/1.6.2/api/python/pyspark.streaming.html)
* [pyspark.mllib](https://spark.apache.org/docs/1.6.0/api/python/pyspark.mllib.html)
* [socket](https://docs.python.org/3/library/socket.html)
* [urllib](https://docs.python.org/3/library/urllib.html)
* [pandas](http://pandas.pydata.org/)
* [pymongo](https://api.mongodb.com/python/current/)
* [tweepy](http://www.tweepy.org/)

## Resources
* [Twitter Trends Analysis using Spark Streaming](http://www.awesomestats.in/spark-twitter-stream/)
* [Interactive Data Visualization with D3.js, DC.js, Python, and MongoDB](http://adilmoujahid.com/posts/2015/01/interactive-data-visualization-d3-dc-python-mongodb/)
* [spark-twitter-sentiment](https://github.com/DhruvKumar/spark-twitter-sentiment/blob/master/sentiment.py)

## License
See the LICENSE file for license rights and limitations (MIT).

