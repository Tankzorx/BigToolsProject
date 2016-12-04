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

seen_subreddit = {}
seen_user = {}

def redditJSON_to_CSV(jsonObj):
	user_csv = ""
	comment_csv = ""
	subreddit_csv = ""
	# Try-catch in case data is illformed
	#try:
	

	body = jsonObj["body"].replace("\n","").replace('\"',"").replace('"',"").replace("\r","").replace("\\","")


	if not (jsonObj["author"] in seen_user):
		seen_user[jsonObj["author"]] = 1
		user_csv += jsonObj["author"] + "\n"
	#user_csv += jsonObj["author"] + "\n"
	comment_csv += jsonObj["id"] \
	+ "," + jsonObj["created_utc"] + "," + str(jsonObj["score"]) + "," + str(jsonObj["downs"]) + ",\"" \
	+ body + '"\n'

	if not (str(jsonObj["subreddit_id"]) in seen_subreddit):
		seen_subreddit[str(jsonObj["subreddit_id"])] = 1
		subreddit_csv += str(jsonObj["subreddit_id"]) + "\n"
	
	return user_csv,comment_csv,subreddit_csv
	#except Exception as e:
	#	raise e
		
def writeToCSV(filename,string):
	with codecs.open(filename,"a",encoding="utf-8") as fileHandle:
		fileHandle.write(string)

def writeCSVFile(filename,outputCommentFile="comment.csv",outputUserFile="user.csv",outputSubredditFile="subreddit.csv",outputRels_POSTED_ON="delMe.csv",outputRels_POSTED_BY="delMe.csv",chunkSize=100000):
	chunkGenerator = readChunk(filename,chunkSize)
	# Empty the files.
	codecs.open(outputUserFile, 'w').close()
	codecs.open(outputCommentFile, 'w').close()
	codecs.open(outputSubredditFile, 'w').close()
	codecs.open(outputRels_POSTED_ON, 'w').close()
	codecs.open(outputRels_POSTED_BY, 'w').close()

	# Insert the headers.
	#writeToCSV(outputUserFile,"userId:ID(User)\n")
	#writeToCSV(outputCommentFile,"commentId:ID(Comment),created_utc,score,downs,body\n")
	#writeToCSV(outputSubredditFile,"subredditId:ID(Subreddit)\n")
	#writeToCSV(outputRels_POSTED_BY,":START_ID(Comment),:END_ID(User)\n")
	#writeToCSV(outputRels_POSTED_ON,":START_ID(Comment),:END_ID(Subreddit)\n")

	# Iterate over all chunks in file:
	for i, chunk in enumerate(chunkGenerator):
		chunkUser = []
		chunkComment = [] 
		chunkSubreddit = []
		chunkRelCommentToUser = []
		chunkRelCommentToSubreddit = []
		for jsonObj in chunk:
			parsedJSON = json.loads(jsonObj)
			user,comment,subreddit = redditJSON_to_CSV(parsedJSON)
			chunkUser.append(user)
			chunkComment.append(comment)
			chunkSubreddit.append(subreddit)
			chunkRelCommentToUser.append(parsedJSON["id"] + "," + parsedJSON["author"] + "\n")
			chunkRelCommentToSubreddit.append(parsedJSON["id"] + "," + parsedJSON["subreddit_id"] + "\n")
		# Done preparing chunk. Now append this chunk to the output files.
		writeToCSV(outputUserFile,"".join(chunkUser))
		writeToCSV(outputCommentFile,"".join(chunkComment))
		writeToCSV(outputSubredditFile,"".join(chunkSubreddit))
		writeToCSV(outputRels_POSTED_BY,"".join(chunkRelCommentToUser))
		writeToCSV(outputRels_POSTED_ON,"".join(chunkRelCommentToSubreddit))

	writeToCSV(outputUserFile,"[deleted]\n")

	return True



def main():
	# Test on small file. Uncomment and run if you're sure the files and folders exists.
	#writeCSVFile("data/RC_2008-01",outputCommentFile="CSV_data/Comments.csv",outputUserFile="CSV_data/Users.csv",outputSubredditFile="CSV_data/Subreddits.csv")
	for i in range(1,13):
		number = i
		if len(str(i)) == 1:
			number = "0" + str(i)
		number = str(number)
		writeCSVFile("data/RC_2008-" + number,\
			outputCommentFile="../bigdata/import/comments" + number + ".csv",\
			outputUserFile="../bigdata/import/users" + number + ".csv",\
			outputSubredditFile="../bigdata/import/Subreddits" + number + ".csv",\
			outputRels_POSTED_ON="../bigdata/import/POSTED_ON" + number + ".csv",\
			outputRels_POSTED_BY="../bigdata/import/POSTED_BY" + number + ".csv")
	
	#writeCSVFile("testdata/double_quote",outputCommentFile="CSV_data/Comments.csv",outputUserFile="CSV_data/Users.csv",outputSubredditFile="CSV_data/Subreddits.csv")
	pass

if __name__ == '__main__':
	main()