import analysis
import os
import sys

def usage():
	print >> sys.stderr, """Usage: _reuters.py CONTEXT PREFIX DIR"""
	sys.exit(1)

context,prefix,dir = sys.argv[1:]

p = analysis.ReutersCorpusParser(context)
p.runAnalysisOnFileList(prefix,[os.sep.join([dir,fileName]) for fileName in os.listdir(dir)])
