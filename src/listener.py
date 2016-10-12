from tweepy.streaming import StreamListener
import json
import time
import socket

class listener(StreamListener):
	
	def __init__(self, csocket):
		self.client_socket = csocket
		# Set a counter to limit the tweets been listened
		#self.counter = 0
		#self.limit = 10

	def on_data(self, data):
		#if self.counter < self.limit:
		# Read the tweet into a dictionary
		tweetdict = json.loads(data)
		# Increment the counter
		#self.counter += 1
		try:
			username = tweetdict['user']['name'].encode('utf-8')
			text = tweetdict['text'].encode('utf-8')
			msg = '---'.encode('utf-8') + username + '+++'.encode('utf-8') + text
			#username = tweetdict['user']['screen_name']
			#hashdict = tweetdict['entities']['hashtags']
			#urldict = tweetdict['entities']['urls']
			#userdict = tweetdict['entities']['user_mentions']
			#tweet = [text, username, hashdict, urldict, userdict]
			print(msg)
			self.client_socket.send(msg)
		except KeyError:
			print('Error.')

		return True
		#else:
		#	return False
	
	def on_error(self, status):
		print(status)

	





