import json
import csv
import re
import codecs
from readChunk import readChunk

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
	user_csv += json["author"] + "\n"
	comment_csv += json["id"] + "," + json["author"] + "," + json["subreddit_id"] \
	+ "," + json["created_utc"] + "," + str(json["score"]) + "," + str(json["downs"]) + ",\"" \
	+ json["body"].replace("\n","\\n").replace("\r","\\r") + "\"\n"

	subreddit_csv += str(json["subreddit_id"]) + "\n"
	return user_csv,comment_csv,subreddit_csv
	#except Exception as e:
	#	raise e
		
def writeToCSV(filename,string):
	with codecs.open(filename,"a",encoding="utf-8") as fileHandle:
		fileHandle.write(string)

def writeCSVFile(filename,outputCommentFile="comment.csv",outputUserFile="user.csv",outputSubredditFile="subreddit.csv",chunkSize=100000):
	chunkGenerator = readChunk(filename,chunkSize)
	# Empty the files.
	codecs.open(outputUserFile, 'w').close()
	codecs.open(outputCommentFile, 'w').close()
	codecs.open(outputSubredditFile, 'w').close()

	# Insert the headers.
	writeToCSV(outputUserFile,"userID\n")
	writeToCSV(outputCommentFile,"commentID, userID, subredditID, created_utc, score, downs, body\n")
	writeToCSV(outputSubredditFile,"subredditID\n")
	# Iterate over all chunks in file:
	for i, chunk in enumerate(chunkGenerator):
		chunkUser = []
		chunkComment = [] 
		chunkSubreddit = []
		for jsonObj in chunk:
			user,comment,subreddit = redditJSON_to_CSV(json.loads(jsonObj))
			chunkUser.append(user)
			chunkComment.append(comment)
			chunkSubreddit.append(subreddit)
		# Done preparing chunk. Now append this chunk to the output files.
		writeToCSV(outputUserFile,"".join(chunkUser)[:-1])
		writeToCSV(outputCommentFile,"".join(chunkComment)[:-1])
		writeToCSV(outputSubredditFile,"".join(chunkSubreddit)[:-1])

	return True



def main():
	# Test on small file. Uncomment and run if you're sure the files and folders exists.
	# writeCSVFile("data/dataSampleFile",outputCommentFile="CSV_data/comment.csv",outputUserFile="CSV_data/user.csv",outputSubredditFile="CSV_data/subreddit.csv")
	pass

if __name__ == '__main__':
	main()