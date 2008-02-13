import data
import getopt
import index
import query
import sys
import time
import unindex

def main(argv=None):
	if argv is None: argv = sys.argv

	def usage():
		print >> sys.stdout,"""\
Usage: python leif OPTIONS

OPTIONS:

--update        Indexer will read in new AnalyzedDocuments and update the index
--data FILE     If running an Update, data is loaded from FILE (a pickle of a list of AnalyzedDocument objects)

--unindex       Indexer will generate source documents (or a best approximation)
--where DIR     If running an Unindex, source documents are created in DIR

--alphabet FILE Pickled termWord hash (generated when the analyzedDocuments were)
--path PATH     Index is located at PATH
--prefix PREFIX Index uses PREFIX in file names
--key KEY       Associate KEY with prefix to prevent opening incorrect Index data
"""
		sys.exit(1)
	
	try:
		options,other_args = getopt.getopt(argv[1:],"",["update","unindex","where=","alphabet=","data=","path=","prefix=","key="])
	except getopt.GetoptError, e:
		print e.msg
		usage()
	
	mode = "QUERY"
	unindexDir = None
	analyzedDocFile = None
	alphabet = None
	path = "."
	prefix = "no-set-prefix"
	key = "no-set-key"

	for option,value in options:
		if option == "--update":
			mode = "UPDATE"
		elif option == "--unindex":
			mode = "UNINDEX"
		elif option == "--where":
			unindexDir = value
		elif option == "--data":
			analyzedDocFile = value
		elif option == "--alphabet":
			alphabet = value
		elif option == "--path":
			path = value
		elif option == "--prefix":
			prefix = value
		elif option == "--key":
			key = value
		else:
			print >> sys.stderr, "Unknown option: %s" % option
			usage()
	
	if alphabet is None: usage()
	import cPickle as pickle
	if mode == "UPDATE" and analyzedDocFile is None: usage()

	termWords = pickle.load(open(alphabet))

	reverseIndex = index.ReverseIndex(path,prefix,key)
	reverseIndex.growthStrategy = index.GrowthStrategyFixedBuffer(1024,3)

	if mode == "UPDATE":
		analysisFile = open(analyzedDocFile)
		while 1:
			try:
				analyzedDocument = pickle.load(analysisFile)
				reverseIndex.post(analyzedDocument)
			except EOFError:
				break

		analysisFile.close()
		reverseIndex.writeToDisk()
	elif mode == "UNINDEX":
		unindex.unindexReverseIndex(termWords,reverseIndex,unindexDir)
	elif mode == "QUERY":
		print >> sys.stderr, "LEIF: Query Test Mode"
		def reverseIndexLookupFunction(_termWord):
			def computedMatchGenerator(_termWord):
				if _termWord in termWords:
					for docIdTermInstanceVector in reverseIndex.lookupTermId(termWords[_termWord]):
						if docIdTermInstanceVector.docId is not None:
							yield data.ComputedMatch(docIdTermInstanceVector.docId,list(docIdTermInstanceVector.termInstancesGenerator))
		
			return data.ComputedMatchVector(computedMatchGenerator(_termWord))
			
		while 1:
			try:
				queryString = raw_input("query> ")
				queryExpression = query.getExpressionTreeFromString(queryString)
				queryResult = query.reduceTopLevel(queryExpression,query.makeInitialEnvironmentFromLookupFunction(reverseIndexLookupFunction))
				if queryResult:
					for computedMatch in queryResult:
						print computedMatch
				else:
					print >> sys.stderr, "Sorry, reducer returned: %s" % repr(queryResult)
			except EOFError:
				break
			except SyntaxError, e:
				print >> sys.stderr, e
				continue

if __name__ == "__main__":
	useProfiler = False
	for argIndex,arg in enumerate(sys.argv):
		if arg == "--profile":
			useProfiler = True
			del sys.argv[argIndex]
			break
	
	if useProfiler:
		import profile
		profile.run("main()")
	else:
		sys.exit(main())
