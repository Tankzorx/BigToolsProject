import time
startTick = time.time()
import json
import csv
import re

from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.linear_model import PassiveAggressiveClassifier
import numpy as np
from readChunk import readChunk
# from imp import reload
# import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')

DEBUG_ON = False
def log(msg):
	if DEBUG_ON:
		print(msg)

def rangifyScore(score):
	if score > 2000:
		return 20
	else:
		# Floor-integer-division. :)
		return score//50

def main():
	# Vectorizer with 2^18 buckets.
	chunkSize = 300000
	n_buckets = 2 ** 19

	vectorizer = HashingVectorizer(decode_error='ignore', n_features=n_buckets,
                               non_negative=True)
	classifier = PassiveAggressiveClassifier()

	#JSONGenerator = readChunk("data/dataSampleFile",chunkSize)
	JSONGenerator = readChunk("data/RC_2007-10",chunkSize)
	#JSONGenerator = readChunk("data/RC_2008-01",chunkSize)
	#JSONGenerator = readChunk("data/RC_2009-12",chunkSize)
	#JSONGenerator = readChunk("data/RC_2012-01",chunkSize)

	JSONArrayTestSet = next(JSONGenerator)
	X_test_text = []
	Y_test = []
	for JSONString in JSONArrayTestSet:
		JSONObject = json.loads(JSONString)
		# Don't care about deleted content.
		if JSONObject["body"] == "[deleted]":
			continue

		X_test_text.append(JSONObject["body"])
		Y_test.append(rangifyScore(int(JSONObject["score"])))

	X_test = vectorizer.transform(X_test_text)
	log("Start till MainLoop timer: " + str(time.time() - startTick))
	generatorTimeTick = time.time()
	# For loop for generators. Smart!
	for i, JSONArray in enumerate(JSONGenerator):
		log("readChunkTimer: " + str(time.time() - generatorTimeTick))
		

		X_train_text = []
		Y_train = []
		extractFeatureTimeTick = time.time()
		for JSONString in JSONArray:

			JSONObject = json.loads(JSONString)
			# Don't care about deleted content.
			if JSONObject["body"] == "[deleted]":
				continue

			X_train_text.append(JSONObject["body"])
			Y_train.append(rangifyScore(int(JSONObject["score"])))

		log("Feature Extract timer: " + str(time.time() - extractFeatureTimeTick))
		tick = time.time()
		X_train = vectorizer.transform(X_train_text)
		log("Vectorize timer:" + str(time.time() - tick))

		tick = time.time()
		classifier.partial_fit(X_train,Y_train,classes=[i for i in range(41)])
		log("Partial fit timer:" + str(time.time() - tick))

		generatorTimeTick = time.time()


	print(classifier.score(X_test,Y_test))



if __name__ == '__main__':
	main()