![Python 3.4](https://img.shields.io/badge/python-3.4-green.svg)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)]()
![release v1.4](https://img.shields.io/badge/release-v1.4-red.svg)
# Twitter Hashtag Tracking

## Motivation
Track specific hashtags or keywords in Twitter, and do real-time analysis on the tweets.

## Run Example
### Configuration
Set your own **src/config.json** file to get [Twitter API](https://apps.twitter.com/) access.
```json
{ "asecret": "XXX...XXX",
  "atoken":  "XXX...XXX",
  "csecret": "XXX...XXX",
  "ckey":    "XXX...XXX"
```

Modify the **conf/parameters.json** file to set the parameters.
```json
{ "hashtag": "#overwatch",
  "DStream": { "batch_interval": "60",
               "window_time": "60",
               "process_times": "60" }
}
```
**Suggestion**: Set batch_interval and window_time the multiple of 60.

### MongoDB Database
Start a mongod process
```
sudo mongod
```
### Model Training
Run Spark jobs to train a Naive Bayes model for later sentiment analysis.
```
$SPARK_HOME/bin/spark-submit src/model.py > log/model.log
```
You can check the accuracy of the trained model in **log/model.log**:
```
>>> Accuracy
0.959944108057755
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

### Twitter API
* Use Twitter API **tweepy** to stream tweets
* Filter out the tweets which contain the specific keywords/hashtag that we want to track.
* Use **TCP/IP socket** to send the fetched tweets to the spark job

### Real-time Analysis
* Use **Spark Streaming** to perform the real-time analysis on the tweets
* Count the number of related tweets for each time interval
* Tweet context **preprocess**
    * Remove all punctuations
    * Set capital letters to lower case
    * Remove **stop words** for better performance
* Find out the most **related keywords**
* Find out the most **related hashtags** 
* **Sentiment analysis**
    * Use **Spark MLlib** to build a **Naive Bayes** model
    * Classify each tweet to be **positive/negative**
    * **Training examples** from [Sanders Analytics](http://www.sananalytics.com/lab/twitter-sentiment/)

### Database
* Use **MongoDB** to store the analysis results

### Visualization
The **Dashboard**.

**Time line** of related tweet counts, **most related hashtags**, **most related keywords**, the ratio of **postive/negative** tweets.
<p align="justify">
  <img src="https://github.com/xuwenyihust/Twitter-Hashtag-Tracking/blob/master/img/visualization.png" width="900"/>
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
* [pickle](https://docs.python.org/3/library/pickle.html)

## Resources
* [Twitter Trends Analysis using Spark Streaming](http://www.awesomestats.in/spark-twitter-stream/)
* [Interactive Data Visualization with D3.js, DC.js, Python, and MongoDB](http://adilmoujahid.com/posts/2015/01/interactive-data-visualization-d3-dc-python-mongodb/)
* [spark-twitter-sentiment](https://github.com/DhruvKumar/spark-twitter-sentiment/blob/master/sentiment.py)

## License
See the LICENSE file for license rights and limitations (MIT).

