# Dashboard 
from flask import Flask
from flask import render_template
from pymongo import MongoClient
import json
from bson import json_util
from bson.json_util import dumps

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DBS_NAME = 'twitter'
COLLECTION_NAME0 = 'keywords'
COLLECTION_NAME1 = 'hashtags'
COLLECTION_NAME2 = 'counts'
FIELDS0 = {'Keyword': True, 'Count': True, '_id': False}
FIELDS1 = {'Hashtag': True, 'Count': True, '_id': False}
FIELDS2 = {'_id': False}

app = Flask(__name__)

@app.route("/")
def index():
	return render_template("dashboard.html")


@app.route("/data/keywords")
def keywords():
	connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
	collection = connection[DBS_NAME][COLLECTION_NAME0]
	projects = collection.find(projection=FIELDS0)
	json_projects = []
	for project in projects:
		json_projects.append(project)
	json_projects = json.dumps(json_projects, default=json_util.default)
	connection.close()
	return json_projects

@app.route("/data/hashtags")
def hashtags():
        connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
        collection = connection[DBS_NAME][COLLECTION_NAME1]
        projects = collection.find(projection=FIELDS1)
        json_projects = []
        for project in projects:
                json_projects.append(project)
        json_projects = json.dumps(json_projects, default=json_util.default)
        connection.close()
        return json_projects

@app.route("/data/counts")
def counts():
        connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
        collection = connection[DBS_NAME][COLLECTION_NAME2]
        projects = collection.find(projection=FIELDS2)
        json_projects = []
        for project in projects:
                json_projects.append(project)
        json_projects = json.dumps(json_projects, default=json_util.default)
        connection.close()
        return json_projects


if __name__ == "__main__":
	app.run(host='0.0.0.0',port=5000,debug=True)




