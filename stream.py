import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import os

def api_access(self):
	config = {}
	with open('config.json') as f:
		config.update(son.load(f))

