import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import os

def api_access():
	config = {}
	with open('config.json') as f:
		config.update(json.load(f))

	return config



def send_data(c_socket):
	auth = OAuthHandler(ckey, csecret)
	auth.set_access_token(atoken, asecret)

	

if __name__ == '__main__':
	# Get the Twitter API keys
	config = api_access()
	asecret = config['asecret']
	atoken = config['atoken']
	csecret = config['csecret']
	ckey = config['ckey']

	# Create a socket object
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	# Get local machine name
	host = socket.gethostbyname(socket.gethostname())
	print('>>> Host Name:\t', host)
	# Reserve a hst for your service
	port = 5555
	# Bind to the port
	s.bind((host, port))
	print('>>> Listening on port:\t%s' % str(port))
	# Wait for client connection
	s.listen(5)
	print('>>> Client connection')
	#s.close()
	# Establish connection with client
	#c, addr = s.accept()

	#print('>>> Received request from ' + str(addr))

	#send_data(c)

