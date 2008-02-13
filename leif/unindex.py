"""
Classes and functions used to unindex an index
"""
import data
import index
import os
import sys

def unindexReverseIndex(alphabet,reverseIndex,path):
	"""Generates a set of documents in path that roughly represent the original documents in the Index"""
	def _deleteDocumentTermCounterString(docCount,termCount):
		deleteString = "[Document %8d Terms %8d]" % (docCount,termCount)
		sys.stdout.write("\b" * len(deleteString))
	def _writeDocumentTermCounterString(docCount,termCount):
		sys.stdout.write("[Document %8d Terms %8d]" % (docCount,termCount))
	outputFileHash = dict()
	for termWord,termId in alphabet.iteritems():
		docCounter = 0
		displayTermWord = termWord[0:14]
		if len(displayTermWord) == 14: displayTermWord = "".join(["<",displayTermWord[:-2],">"])
		sys.stdout.write("Unindexing term %14s " % displayTermWord)
		_writeDocumentTermCounterString(0,0)
		for docIdTermInstanceVector in reverseIndex.lookupTermId(termId):
			termCounter = 0
			_deleteDocumentTermCounterString(docCounter,termCounter)
			docCounter += 1
			_writeDocumentTermCounterString(docCounter,termCounter)
			docId = docIdTermInstanceVector.docId
			if docId not in outputFileHash:
				outputFileName = os.sep.join([path,str(docId) + ".fwd"])
				outputFileHash[docId] = outputFileName
			fp = open(outputFileHash[docId],"ab")

			for termInstance in docIdTermInstanceVector.termInstancesGenerator:
				_deleteDocumentTermCounterString(docCounter,termCounter)
				termCounter += 1
				_writeDocumentTermCounterString(docCounter,termCounter)
				print >> fp, "%d %s" % (termInstance.position,termWord)
			fp.close()

		sys.stdout.write(" DONE\n")
	
	for fileName in outputFileHash.values():
		fp = open(fileName,"rb")
		fileTerms = sorted([(int(position),word[:-1]) for position,word in [line.split(" ",1) for line in fp]])
		fp.close()
		print >> sys.stdout, "Reorganizing: %s" % fileName
		fp = open(fileName,"wb")
		for termPosition,termWord in fileTerms:
			fp.write(termWord + " ")
		fp.close()
