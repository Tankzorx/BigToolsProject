import json
import csv
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# If the file is fully read, next() calls will simply return the empty string.
def readJSONChunk(filename,lines_to_yield=1):
	jsonObjects = []
	lines_read = 0
	with open(filename) as fileHandle:
		while True:
			line = fileHandle.readline()
			lines_read += 1
			if line:
				#print line
				jsonObjects.append(json.loads(line))
				if lines_read >= lines_to_yield:
					# We're here because we hit our line cap.
					yield jsonObjects
					jsonObjects = []
					lines_read = 0
			else:
				# We're here because we hit EOF.
				if len(jsonObjects) == 0:
					return
				else:
					yield jsonObjects
					jsonObjects = []
					lines_read = 0
	
# Returns 3 different CSV snippets. 1 for user, 1 for comment and 1 for subreddit.
# users:
# author
#
# comment:
# id, author, subreddit_id, created_utc, score, downs, body
#
# subreddit:
# subreddit_id
#
def redditJSON_to_CSV(json):
	user_csv = ""
	comment_csv = ""
	subreddit_csv = ""
	# Try-catch in case data is illformed
	#try:
	user_csv += unicode(json["author"]) + "\n"
	comment_csv += unicode(json["id"]) + "," + unicode(json["author"]) + "," + unicode(json["subreddit_id"]) \
	+ "," + unicode(json["created_utc"]) + "," + unicode(json["score"]) + "," + unicode(json["downs"]) + "," + json["body"].replace("\n","\\n").replace("\r","\\r") + "\n"

	subreddit_csv += str(json["subreddit_id"]) + "\n"
	return user_csv,comment_csv,subreddit_csv
	#except Exception as e:
	#	raise e
		
def writeToCSV(filename,string):
	with open(filename,"a") as fileHandle:
		fileHandle.write(unicode(string))


def main():
	k = readJSONChunk("data/RC_2007-10",1000)
	#k = readJSONChunk("data/dataSampleFile",100000)
	userCSVPath = "CSV_data/user.csv"
	commentCSVPath = "CSV_data/comment.csv"
	subredditCSVPath = "CSV_data/subreddit.csv"

	# This empties the CSV files!
	open(userCSVPath, 'w').close()
	open(commentCSVPath, 'w').close()
	open(subredditCSVPath, 'w').close()

	# Insert the headers.
	writeToCSV(userCSVPath,"userID\n")
	writeToCSV(commentCSVPath,"commentID, userID, subredditID, created_utc, score, downs, body\n")
	writeToCSV(subredditCSVPath,"subredditID\n")

	while True:
		#print "I'm alive.."
		nextChunk = []
		try:
			#print "Start read"
			nextChunk = k.next()
			#print "End read"
		except Exception as e:
			# print e
			pass


		if len(nextChunk) == 0:
			break

		user_csv = []
		comment_csv = []
		subreddit_csv = []
		for JSONObject in nextChunk:
			user,comment,subreddit = redditJSON_to_CSV(JSONObject)
			user_csv.append(user)
			comment_csv.append(comment)
			subreddit_csv.append(subreddit)

		writeToCSV(userCSVPath,"".join(user_csv)[:-1])
		writeToCSV(commentCSVPath,"".join(comment_csv)[:-1])
		writeToCSV(subredditCSVPath,"".join(subreddit_csv)[:-1])


	

	

if __name__ == '__main__':
	main()