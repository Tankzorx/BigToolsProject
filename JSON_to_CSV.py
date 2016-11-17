import json
import csv

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
			

def main():
	try:
		k = readJSONChunk("data/dataSampleFile",1)
		print len(k.next())
		print len(k.next())
		print len(k.next())
	except Exception, e:
		if e == "StopIteration":
			print "Stop Iteration!!"
		return
	finally:
		return
	

if __name__ == '__main__':
	main()