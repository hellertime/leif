import analysis
import itertools
import lazy
import os
import sys

def usage():
	print >> sys.stderr, """Usage: _reuters.py CONTEXT PREFIX DIR BLOCK_SIZE"""
	sys.exit(1)

try:
	context,prefix,dir,block_size = sys.argv[1:]
except:
	usage()

block_size = int(block_size)
if block_size == -1:
	p = analysis.ReutersCorpusParser(context)
	p.runAnalysisOnFileList(prefix,lazy.recursiveListdir(dir,True))
else:
	passCount = 0
	continueProcessing = True
	fileGenerator = lazy.peekable(lazy.recursiveListdir(dir,True))
	while continueProcessing:
		passCount += 1
		sys.stderr.write("Starting pass #%d" % passCount)
		p = analysis.ReutersCorpusParser(context)
		fileList = itertools.islice(fileGenerator,0,block_size)
		p.runAnalysisOnFileList(prefix + "-%d" % passCount,fileList)
		sys.stderr.write(" DONE\n")
		try:
			fileGenerator.peek()
		except StopIteration:
			continueProcessing = False
