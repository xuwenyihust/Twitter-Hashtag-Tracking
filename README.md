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
## Prerequisite


## Resources
* [Twitter Trends Analysis using Spark Streaming](http://www.awesomestats.in/spark-twitter-stream/)
* [Interactive Data Visualization with D3.js, DC.js, Python, and MongoDB](http://adilmoujahid.com/posts/2015/01/interactive-data-visualization-d3-dc-python-mongodb/)
* [spark-twitter-sentiment](https://github.com/DhruvKumar/spark-twitter-sentiment/blob/master/sentiment.py)

## License
See the LICENSE file for license rights and limitations (MIT).

