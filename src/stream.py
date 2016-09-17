#import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import os
from listener import listener

def api_access():
	config = {}
	with open('src/config.json') as f:
		config.update(json.load(f))

	return config

def hashtag_load():
	hashtag = ''
	with open('hashtag.json') as f:
		h = json.load(f)
		hashtag = h['hashtag']
	return hashtag

def send_data(c_socket, hashtag):
	auth = OAuthHandler(ckey, csecret)
	auth.set_access_token(atoken, asecret)

	twitter_stream = Stream(auth, listener(c_socket))
	twitter_stream.filter(track=[hashtag], languages=['en'])
	

if __name__ == '__main__':
	# Get the Twitter API keys
	config = api_access()
	asecret = config['asecret']
	atoken = config['atoken']
	csecret = config['csecret']
	ckey = config['ckey']

	# Load the hashtag to be tracked
	hashtag = hashtag_load()

	# Create a TCP/IP socket object
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	#s.settimeout(5)
	
	# Get local machine name
	host = socket.gethostbyname(socket.gethostname())
	#host = '192.168.200.146'
	#host = socket.gethostname()
	print('>>> Host Name:\t%s' % str(host))
	# Reserve a hst for your service
	port = 5555
	server_addr = (host, port)
	# Bind the socket with the server address
	s.bind(server_addr)
	print('>>> Listening on port:\t%s' % str(port))
	# Calling listen() puts the socket into server mode
	s.listen(5)
	print('>>> Waiting for client connection')
	# accept() waits for an incoming connection
	# Establish connection with client
	client, addr = s.accept()

	print('>>> Received request from ' + str(addr))

	send_data(client, hashtag)

	s.close()


