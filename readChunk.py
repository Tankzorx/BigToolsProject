import codecs

# Function to read a file in chunks.
def readChunk(filename,lines_to_yield=1):
	jsonObjects = []
	lines_read = 0
	with codecs.open(filename,encoding="utf-8") as fileHandle:
		while True:
			line = fileHandle.readline()
			lines_read += 1
			if line:
				jsonObjects.append(line)
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