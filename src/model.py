# Train the ML model
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
import pickle


def parseLine(line):
	parts = line.split('\t')
	label = int(parts[0])
	tweet = parts[1]
	# Tokenize
	# Split a document into a collection of words
	words = tweet.strip().split(" ")
	return (label, words)


def main(sc):

	data = sc.textFile('data/train.txt').map(parseLine)
	#print(data.take(10))

	# Train/Test split
	training, test = data.randomSplit([0.7, 0.3], seed = 0)

	# TF-IDF
	# TF
	# Features will be hashed to indexes
	# And the feature(term) frequencies will be calculated
	hashingTF = HashingTF()
	# For each training example
	tf_training = training.map(lambda tup: hashingTF.transform(tup[1]))
	# IDF
	# Compute the IDF vector
	idf_training = IDF().fit(tf_training)
	# Scale the TF by IDF
	tfidf_training = idf_training.transform(tf_training)

	# (SparseVector(1048576, {110670: 1.5533, ...), 0)
	tfidf_idx = tfidf_training.zipWithIndex()
	# (['The', 'Da', 'Vinci', 'Code', 'book', 'is', 'just', 'awesome.'], 0)
	training_idx = training.zipWithIndex()

	# Reverse the index and the SparseVector
	idx_training = training_idx.map(lambda line: (line[1], line[0]))
	idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
	#print(idx_training.take(10))

	# rdd.join: (K,V).join(K,W) -> (K, (V,W)) 
	# idx_tfidf has no info about lables(0/1)
	# but idx_training has
	joined_tfidf_training = idx_training.join(idx_tfidf)
	training_labeled = joined_tfidf_training.map(lambda tup: tup[1])
	training_labeled = training_labeled.map(lambda x: LabeledPoint(x[0][0], x[1]))
	#print(training_labeled.take(10))

	# Train a naive Bayes model
	model = NaiveBayes.train(training_labeled, 1.0)

	# Test the model
	tf_test = test.map(lambda tup: hashingTF.transform(tup[1]))
	idf_test = IDF().fit(tf_test)

	tfidf_test = idf_test.transform(tf_test)
	tfidf_idx = tfidf_test.zipWithIndex()
	test_idx = test.zipWithIndex()
	idx_test = test_idx.map(lambda line: (line[1], line[0]))
	idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
	joined_tfidf_test = idx_test.join(idx_tfidf)

	test_labeled = joined_tfidf_test.map(lambda tup: tup[1])
	labeled_test_data = test_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))
	#print(labeled_test_data.take(2))
	# Apply the trained model on Test data
	predictionAndLabel = labeled_test_data.map(lambda p : (model.predict(p.features), p.label))
	#print(predictionAndLabel.take(10))

	# Calculate the accuracy
	accuracy = 1.0 * predictionAndLabel.filter(lambda x: x[0] == x[1]).count() / labeled_test_data.count()

	print('>>> Accuracy')
	print(accuracy)

	#model.save(sc, '/model')
	output = open('src/model/model.ml', 'wb')
	pickle.dump(model,output)


if __name__=="__main__":
	# Define Spark configuration
	conf = SparkConf()
	conf.setMaster("local[4]")
	conf.setAppName("Twitter-Hashtag-Tracking")
	# Initialize a SparkContext
	sc = SparkContext(conf=conf)
	# Initialize sparksql context
	# Will be used to query the trends from the result.
	sqlContext = SQLContext(sc)

	main(sc)


