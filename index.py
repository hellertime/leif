"""
Classes and functions dealing with the Index structure
"""
import data
import exceptions
import mmap
import os
import pickle_tools
import Queue
import struct
import sys
import threading
import time

DefaultMetadataFileSuffix = ".meta"

class ReverseIndexKeyError(exceptions.Exception):
	"""raise if the indexKey does not match its expected value"""

def openIndexPartition(name,path,metadataFileSuffix=DefaultMetadataFileSuffix,indexKey=None):
	"""Creates the appropriate partition based on the path

	MemoryPartitons will be created if the path starts with :memory:"""
	if path.startswith(":memory:"):
		path = path[len(":memory:"):]
		return MemoryPartition(name,path,_indexKey=indexKey)
	else:
		return ExternalPartition(name,path,_metadataFileSuffix=metadataFileSuffix,_indexKey=indexKey)

class MemoryPartition(object):
	"""MemoryPartition keeps all index data in RAM.
	It can optionally be backed by a permanent file which is loaded at __init__"""
	__slots__ = ["name","path","indexKey","termInstanceLimit","termIdHash"]
	def __init__(self,_name,_path,_indexKey=None):
		self.name = _name
		self.path = _path # can be None
		self.indexKey = _indexKey
		self.termInstanceLimit = None
		self.termIdHash = dict()

		self.__pickle_init__()
	
	def __pickle_init__(self):
		if self.path:
			try:
				print >> sys.stderr, "MemoryPartition data found at %s" % self.path
				key = self.indexKey
				pickle_tools.pickle_load_attrs(self,self.path) # can set indexKey
				if key and key != self.indexKey:
					raise ReverseIndexKeyError("MemoryPartition %s provided incorrect indexKey" % self.path)
			except IOError:
				print >> sys.stderr, "Unable to load MemoryPartition data from %s" % self.path
	
	def writeToDisk(self):
		if self.path:
			print >> sys.stderr, "Pickling MMP to %s" % self.path
			pickle_tools.pickle_dump_attrs(self,self.path,"termInstanceLimit","termIdHash","indexKey")
	
	def zeroAllData(self):
		self.termIdHash = dict()
		# remove disk based data
		if self.path and os.path.exists(self.path): os.unlink(self.path)
	
	@property
	def termInstanceCount(self):
		return sum(map(lambda termInstance: termInstance.termInstanceCount,self.termIdHash.values()))
	
	def reachedTermInstanceLimit(self):
		if self.termInstanceLimit: return self.termInstanceLimit == self.termInstanceCount
		return False
	
	def addTermInstance(self,termId,docId,position,extent=0):
		if termId not in self.termIdHash:
			self.termIdHash[termId] = data.DocIdTermInstanceTable()
		self.termIdHash[termId].insertTermInstanceRecord(docId,data.TermInstance(position,extent))
	
	def lookupTermId(self,termId):
		if termId in self.termIdHash:
			return data.readUncompressedDocIdTermInstanceTable(self.termIdHash[termId])
		return data.nullUncompressedDocIdTermInstanceTable()
	
	def deleteTermId(self,termId):
		if termId in self.termIdHash: del self.termIdHash[termId]
	
	def deleteDocId(self,termId,docId):
		if termId in self.termIdHash: self.termIdHash[termId].deleteDocId(docId)
	
	def estimateSizeOnDisk(self):
		"""If we are to serialize this data how much room might we need"""
		_size = data.estimateSizeOfDocIdTermInstanceTable
		return sum(map(_size,self.termIdHash.values()))
	
	def compressTermIdData(self,termId):
		return data.compressDocIdTermInstanceTable(self.termIdHash[termId])
	
	def __contains__(self,termId): return termId in self.termIdHash
	
class ExternalPartition(object):
	"""ExternalPartition uses an on disk file to store compressed DocIdTermInstanceTable instances
	In memory it must maintan only enough information to read the proper table for a termId
	This in-memory data must be explicitly preserved to disk, and will be loaded at __init___"""
	__slots__ = ["name","path","indexKey","metadataFileSuffix","termInstanceLimit","termIdHash","fp","mmap"]
	def __init__(self,_name,_path,_metadataFileSuffix=DefaultMetadataFileSuffix,_indexKey=None):
		self.name = _name
		self.path = _path
		self.indexKey = _indexKey
		self.metadataFileSuffix = _metadataFileSuffix
		self.termInstanceLimit = None
		self.termIdHash = dict()

		self.__pickle_init__()
		self.__mmap_init__()
	
	def __pickle_init__(self):
		metadataPath = self.path + self.metadataFileSuffix
		if os.path.exists(metadataPath):
			try:
				print >> sys.stderr, "ExternalPartition metadata found at %s" % self.path
				key = self.indexKey
				pickle_tools.pickle_load_attrs(self,metadataPath)
				if key and key != self.indexKey:
					raise ReverseIndexKeyError("ExternalPartition metadata %s provided incorrect indexKey" % metadataPath)
			except IOError:
				print >> sys.stderr, "Unable to load ExternalPartition metadata from %s" % metadataPath
	
	def writeToDisk(self):
		metadataPath = self.path + self.metadataFileSuffix
		pickle_tools.pickle_dump_attrs(self,metadataPath,"termInstanceLimit","termIdHash","indexKey")
	
	def zeroAllData(self):
		self.termIdHash = dict()
		metadataPath = self.path + self.metadataFileSuffix
		if os.path.exists(metadataPath): os.unlink(metadataPath)
		# truncate the index, but do not remove it from disk
		open(self.path,"wb").close()
	
	@property
	def termInstanceCount(self):
		return sum(map(lambda termInstanceHeader: termInstanceHeader.termInstanceCount,self.termIdHash.values()))
	
	def reachedTermInstanceLimit(self):
		if self.termInstanceLimit: return self.termInstanceLimit == self.termInstanceCount
		return False
	
	def lookupTermId(self,termId):
		if termId in self.termIdHash: 
			return data.decompressDocIdTermInstanceTable(self.mmap,self.termIdHash[termId])
		return data.nullUncompressedDocIdTermInstanceTable()
	
	def deleteTermId(self,termId):
		"""does not remove data from index, just drops the reference in the termIdHash to prevent lookup"""
		if termId in self.termIdHash: del self.termIdHash[termId]
	
	def estimateSizeOnDisk(self):
		return sum(map(lambda header: header.length,self.termIdHash.values()))
	
	def compressTermIdData(self,termId):
		header = self.termIdHash[termId]
		return data.readCompressedDocIdTermInstanceTable(self.mmap,header)
	
	def __contains__(self,termId): return termId in self.termIdHash
	
	def __mmap_init__(self):
		"""calling this opens the index file and mmaps it into memory
		the attributes "fp" and "mmap" do not exist before calling this
		it must be called after every index file size change
		This will fail is self.path is missing
		"""
		if os.path.exists(self.path):
			fileSize = os.stat(self.path).st_size
			if fileSize > 0:
				self.fp = open(self.path,"rb")
				self.mmap = mmap.mmap(self.fp.fileno(),fileSize,mmap.MAP_SHARED,mmap.PROT_READ)
	
	def mergePartitions(self,termIdList,*partitions):
		"""Merge the data from partitions into self
		This must be a method on an ExternalPartition, MemoryPartitions have no concept of merging
		termIdList must contain the sorted list of all termIds in all partitions
		the resulting merged partition will contain one entry for each termId
		"""
		# seek constants... no need to import them from wherever...
		SEEK_END = 2
		SEEK_CUR = 1
		# internal functions to handle some preliminaries
		# makes the main merge code easier to understand, sacrificing overval function length
		def _growPartitionFile(howMuch):
			"""extend the disk partition by howMuch"""
			if os.path.exists(self.path): # prevent trucating existing file
				print >> sys.stderr, "Extending ExternalPartition %s" % self.path
				fp = open(self.path,"rb+")
			else:
				print >> sys.stderr, "Creating ExternalPartiton %s" % self.path
				fp = open(self.path,"wb")

			fp.seek(0,SEEK_END)
			previousSize = fp.tell()
			fp.seek(howMuch - 1,SEEK_CUR)
			fp.write('\x00')
			newSize = fp.tell()
			print >> sys.stderr, "ExternalPartition grew %d bytes, has size %s" % (newSize-previousSize,newSize)
			fp.close()
			self.__mmap_init__()
		def _relocateDocIdTermInstanceTables():
			"""Relocates the existing tables to the end of the file maintaining proper offsets"""
			rp = open(self.path,"rb")
			wp = open(self.path,"rb+")
			wp.seek(0,SEEK_END)

			for termId in reversed(sorted(self.termIdHash)):
				header = self.termIdHash[termId]
				rp.seek(header.offset)
				wp.seek(-header.length,SEEK_CUR)
				newOffset = wp.tell()
				wp.write(rp.read(header.length))
				wp.seek(newOffset)
				self.termIdHash[termId].offset = newOffset

			rp.close()
			wp.close()
		# Main Merge Logic
		spaceNeeded = sum(map(lambda partition: partition.estimateSizeOnDisk(),partitions))
		_growPartitionFile(spaceNeeded)
		_relocateDocIdTermInstanceTables()
		wp = open(self.path,"rb+")

		for termId in termIdList:
			partitionsHoldingTermId = list()
			for partition in partitions:
				if termId in partition:
					partitionsHoldingTermId.append(partition)

			# always add self last
			if termId in self: partitionsHoldingTermId.append(self)
			if len(partitionsHoldingTermId) == 0: continue
			else:
				newOffset = wp.tell()

			if len(partitionsHoldingTermId) == 1:
				partition = partitionsHoldingTermId[0]
				#print >> sys.stderr, "Merge single instance of termId %d from %s" % (termId,partition.name)
				header,compressedData = partition.compressTermIdData(termId)
				wp.write(compressedData)
				header.offset = newOffset
				self.termIdHash[termId] = header

				if partition is not self: partition.deleteTermId(termId)
			else:
				table = data.DocIdTermInstanceTable()
				for partition in partitionsHoldingTermId:
					#print >> sys.stderr, "Merge multi instance termId %s from %s" % (termId,partition.name)
					for docIdTermInstanceVector in partition.lookupTermId(termId):
						docId = docIdTermInstanceVector.docId
						for termInstance in docIdTermInstanceVector.termInstancesGenerator:
							table.insertTermInstanceRecord(docId,termInstance)

					if partition is not self: partition.deleteTermId(termId)

				header,compressedData = data.compressDocIdTermInstanceTable(table) 
				wp.write(compressedData)
				header.offset = newOffset
				self.termIdHash[termId] = header

		wp.truncate()
		print >> sys.stderr, "ExternalPartition was truncated to size %d" % wp.tell()
		wp.close()
		self.__mmap_init__()

class GrowthStrategyFixedBuffer(object):
	"""a partition growth strategy where we merge into the next partition when the previous
	growns past a certain ratio. Each partition however has a fixed max size, known when its created"""
	__slots__ = ["bufferSizeFactor","growthFactor"]
	def __init__(self,_bufferSizeFactor,_growthFactor):
		self.bufferSizeFactor = _bufferSizeFactor
		self.growthFactor = _growthFactor
	
	def computeTermInstanceLimitForPartitionK(self,k):
		"""returns an integer value based on k and b and r
		The use of b and r as variables come from the paper in this bibtex cite:
		@inproceedings{1099739,
		author = {Nicholas Lester and Alistair Moffat and Justin Zobel},
		title = {Fast on-line index construction by geometric partitioning},
		booktitle = {CIKM '05: Proceedings of the 14th ACM international conference on Information and knowledge management},
		year = {2005},
		isbn = {1-59593-140-6},
		pages = {776--783},
		location = {Bremen, Germany},
		doi = {http://doi.acm.org/10.1145/1099554.1099739},
		publisher = {ACM},
		address = {New York, NY, USA},
		}
		"""
		b = self.bufferSizeFactor
		r = self.growthFactor
		if k == 0: return b
		else: return ((r-1)*(r**(k-1)))*b
	
	def mergePartitions(self,termIdList,partitions,partitionConstructor):
		mergeIntoPartitionK = len(partitions)
		for partitionK in xrange(1,mergeIntoPartitionK):
			termInstanceCount = sum(map(lambda partition: partition.termInstanceCount,partitions[:partitionK+1]))
			if termInstanceCount <= self.computeTermInstanceLimitForPartitionK(partitionK):
				mergeIntoPartitionK = partitionK
				break

		if mergeIntoPartitionK == len(partitions):
			newPartition = partitionConstructor(mergeIntoPartitionK)
			newPartition.termInstanceLimit = self.computeTermInstanceLimitForPartitionK(mergeIntoPartitionK)
			partitions.append(newPartition)

		partitions[mergeIntoPartitionK].mergePartitions(termIdList,*partitions[:mergeIntoPartitionK])
		for k in xrange(mergeIntoPartitionK):
			partitions[k].zeroAllData()

class ReverseIndex(object):
	"""Brings together the Memory and External partitions in to a single interface"""
	def __init__(self,_path,_partitionPrefix,_indexKey):
		self.path = _path
		self.partitionPrefix = _partitionPrefix
		self.indexKey = _indexKey
		self.growthStrategy = GrowthStrategyFixedBuffer(4096,3)
		self.makePartitionName = lambda name: os.sep.join([self.path,self.partitionPrefix + ".%s" % name])

		mmp = openIndexPartition("MMP",":memory:%s" % self.makePartitionName("MMP"),indexKey=self.indexKey)
		mmp.termInstanceLimit = self.growthStrategy.computeTermInstanceLimitForPartitionK(0)

		self.partitions = [mmp]
		self.externalPartitionCount = 0
		self.lexicon = dict()
		self.termCount = 0

		self.__pickle_init__()
		self.openAllExternalPartitions()

		self.__document_ingress_init__()
		self.__posting_ingress_init__()

	def __pickle_init__(self):
		path = self.makePartitionName("LEX")
		if os.path.exists(path):
			try:
				print >> sys.stderr, "ReverseIndex metadata found at %s" % path
				pickle_tools.pickle_load_attrs(self,path)
			except IOError:
				print >> sys.stderr, "Unable to load ReverseIndex metadata from %s" % path
	
	def openAllExternalPartitions(self):
		print >> sys.stderr, "ReverseIndex has %d external partitions to open" % (self.externalPartitionCount)
		for k in xrange(self.externalPartitionCount):
			k = k + 1
			self.partitions.append(openIndexPartition("EXP%d"%k,self.makePartitionName("EXP%d"%k),indexKey=self.indexKey))
	
	def writeToDisk(self):
		# THIS IS A HACK!!! WE NEED BETTER merge synronization
		busyLoopCounter = 0
		while not (self.documentQueue.empty() and self.postingQueue.empty()):
			time.sleep(30)
			if busyLoopCounter % 5 == 0:
				print >> sys.stderr, "WriteToDisk waiting on queues..."
			busyLoopCounter += 1

		print >> sys.stderr, "Writing to disk..."

		path = self.makePartitionName("LEX")
		pickle_tools.pickle_dump_attrs(self,path,"externalPartitionCount","lexicon","termCount")
		for partition in self.partitions:
			partition.writeToDisk()
	
	def __document_ingress_init__(self):
		"""creates the post() method so that analyzed docs can be added to the index
		also created the ingress thread and associated data
		"""
		def _documentIngressThread(self):
			willBlock = True
			while 1:
				analyzedDocument = self.documentQueue.get(willBlock)
				for position,analyzedTerm in enumerate(analyzedDocument.analyzedTermList):
					for termId,extent in analyzedTerm.instanceSet:
						if termId not in self.lexicon:
							self.lexicon[termId] = self.termCount
							self.termCount += 1
						termId = self.lexicon[termId]
						self.postingQueue.put((termId,analyzedDocument.docId,position,extent))

		self.post = lambda analyzedDocument: self.documentQueue.put(analyzedDocument)
		self.documentQueue = Queue.Queue(-1)
		self.documentIngressThread = threading.Thread(target = _documentIngressThread,args = (self,))
		self.documentIngressThread.setDaemon(True)
		self.documentIngressThread.start()
	
	def __posting_ingress_init__(self):
		"""pulls data from the documentQueue and puts it in the index, while managing the indexes growth"""
		def _lexiconTermIds(self):
			return [self.lexicon[termId] for termId in sorted(self.lexicon)]

		def _postingIngressThread(self):
			willBlock = True
			while 1:
				termId,docId,position,extent = self.postingQueue.get(willBlock)
				if self.partitions[0].reachedTermInstanceLimit():
					print >> sys.stderr, "Extending partitions"
					def _externalPartitionConstructor(k):
						partitionName = "EXP%d" % k
						return openIndexPartition(partitionName,self.makePartitionName(partitionName),indexKey=self.indexKey)
					self.growthStrategy.mergePartitions(_lexiconTermIds(self),self.partitions,_externalPartitionConstructor)
					self.externalPartitionCount = len(self.partitions) - 1
				self.partitions[0].addTermInstance(termId,docId,position,extent)

		self.postingQueue = Queue.Queue(-1)
		self.postingIngressThread = threading.Thread(target = _postingIngressThread,args = (self,))
		self.postingIngressThread.setDaemon(True)
		self.postingIngressThread.start()
	
	def lookupTermId(self,termId):
		if termId in self.lexicon:
			termId = self.lexicon[termId]
			return data.joinUncompressedDocIdTermInstanceTableReaders([partition.lookupTermId(termId) for partition in self.partitions])
		else:
			return data.nullUncompressedDocIdTermInstanceTable()

# A Test Mode
if __name__ == "__main__":
	index = ReverseIndex("./partition-test","test-prefix","shabba")
	d = ["the fat rat","the fast cat","cat ate rat","cat got fat","whoa that cat","theres no rat","imagine that","cat got rat","cat cat rat","rat cat cat"]
	termWords = dict()
	nextTermId = 0
	for docId,doc in enumerate(d):
		analyzedDocument = data.AnalyzedDocument(docId)
		for termWord in doc.split():
			if termWord not in termWords:
				termWords[termWord] = nextTermId
				nextTermId += 1

			analyzedTerm = data.AnalyzedTerm()
			analyzedTerm.addTermIdWithOptionalExtent(termWords[termWord])
			analyzedDocument.appendAnalyzedTerm(analyzedTerm)

		index.post(analyzedDocument)
	
	while 1:
		try:
			termWord = raw_input("termWord> ")
			if termWord in termWords:
				for docIdTermInstanceVector in index.lookupTermId(termWords[termWord]):
					print "%d -> %d -> %s" % (termWords[termWord],docIdTermInstanceVector.docId,repr(list(docIdTermInstanceVector.termInstancesGenerator)))
		except EOFError:
			break
