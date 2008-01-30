import cPickle as pickle
import data
import os
import pickle_tools
import sys
import xml.sax.handler

def deadSimpleNormalizer(inputString):
	"""Brain-dead normalizer, splits on whitespace, deletes punctuation from end"""
	for token in inputString.split():
		if token[-1] in (",",";",".","?","!"):
			yield token[:-1]
		else:
			yield token

class ReutersCorpusParser(object):
	class ReutersCorpusHandler(xml.sax.ContentHandler):
		"""Can parse Reuters Corpus XML into an intermediate S-expr format, which is then converted to AnalyzedDcouments and termWords"""
		def __init__(self):
			self.docId = None
			self.termTree = list()
			self.nodeStack = None		

		def startElement(self,name,attributes):
			if name == "newsitem": self.docId = int(attributes["itemid"])
			if self.nodeStack is None:
				self.nodeStack = [[name]]
			else:
				newNode = [name]
				self.nodeStack[-1].append(newNode)
				self.nodeStack.append(newNode)
		
		def characters(self,data):
			for token in deadSimpleNormalizer(data):
				self.nodeStack[-1].append(token)

		def endElement(self,name):
			node = self.nodeStack.pop()
			if len(self.nodeStack) == 0:
				self.termTree.append(node)
				self.nodeStack = None

	def __init__(self,contextPath):
		self.path = contextPath
		self.nextTermId = 0
		self.termWords = dict()
		self.parser = xml.sax.make_parser()
		self.handler = None

		if os.path.exists(self.path):	pickle_tools.pickle_load_attrs(self,self.path)
	
	def saveContext(self):
		pickle_tools.pickle_dump_attrs(self,self.path,"nextTermId","termWords")
	
	def parseOneDocument(self,path):
		self.handler = ReutersCorpusParser.ReutersCorpusHandler()
		self.parser.setContentHandler(self.handler)
		self.parser.parse(path)
	
	def addTermWordToTermWords(self,termWord):
		if str(termWord) not in self.termWords:
			self.termWords[str(termWord)] = self.nextTermId
			self.nextTermId += 1
	
	def getTermWordTermId(self,termWord):
		termWord = str(termWord)
		if termWord not in self.termWords:
			self.termWords[termWord] = self.nextTermId
			self.nextTermId += 1
		return self.termWords[termWord]
	
	def analyzeDocument(self):
		"""Recursively inspect the parsed document to build an analyzed document"""
		def nodeLength(node):
			nodeLen = 0
			for element in node:
				if type(element) == type([]):
					for innerElement in element:
						nodeLen += nodeLength(innerElement)
				else:
					nodeLen += 1

			return nodeLen

		def termWalker(self,node,analyzedDocument):
			if type(node) == type([]):
				nodeLen = nodeLength(node)
				nodeName,nodeChildren = node[0],node[1:]
				analyzedTerm = data.AnalyzedTerm()
				analyzedTerm.addTermIdWithOptionalExtent(self.getTermWordTermId(nodeName),nodeLen)
				if len(nodeChildren) and type(nodeChildren[0]) != type([]):
					firstChild,nodeChildren = nodeChildren[0],nodeChildren[1:]
					analyzedTerm.addTermIdWithOptionalExtent(self.getTermWordTermId(firstChild))
				analyzedDocument.appendAnalyzedTerm(analyzedTerm)
				for childNode in nodeChildren:
					termWalker(self,childNode,analyzedDocument)
			else:
				analyzedTerm = data.AnalyzedTerm()
				analyzedTerm.addTermIdWithOptionalExtent(self.getTermWordTermId(node))
				analyzedDocument.appendAnalyzedTerm(analyzedTerm)

		analyzedDocument = data.AnalyzedDocument(self.handler.docId)
		for node in self.handler.termTree:
			termWalker(self,node,analyzedDocument)
		return analyzedDocument
	
	def runAnalysisOnFileList(self,outputPrefix,fileList):
		outputFile = open(outputPrefix + ".docs","wb")
		for fileName in fileList:
			self.parseOneDocument(fileName)
			analyzedDocument = self.analyzeDocument()
			pickle.dump(analyzedDocument,outputFile,-1)

		outputFile.close()
		termWordsFile = open(outputPrefix + ".alphabet","wb")
		pickle.dump(self.termWords,termWordsFile,-1)
		termWordsFile.close()
		self.saveContext()
