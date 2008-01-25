"""
Classes and functions dealing with the Index structure
"""
import data
import exceptions
import mmap
import os
import pickle_tools
import struct
import sys

DefaultMetadataFileSuffix = ".meta"

class ReverseIndexKeyError(exceptions.Exception):
	"""raise if the indexKey does not match its expected value"""

def openIndexPartition(name,path,metadataFileSuffix=DefaultMetadataFileSuffix,indexKey=None):
	"""Creates the appropriate partition based on the path

	MemoryPartitons will be created is the path starts with :memory:"""
	if path.startswith(":memory:"):
		path = path[len(":memory:"):]
		return MemoryPartition(name,path,_indexKey=indexKey)
	else:
		return ExternalPartition(name,path,_metadataFileSuffix=metadataFileSuffix,_indexKey=indexKey)

class MemoryPartition(object):
	"""MemoryPartition keeps all index data in RAM.
	It can optionally be backed by a permanent file which is loaded at __init__"""
	__slots__ = ["name","path","indexKey","termInstanceLimit","termIdHash","termInstanceCount"]
	def __init__(self,_name,_path,_indexKey=None):
		self.name = _name
		self.path = _path # can be None
		self.indexKey = _indexKey
		self.termInstanceLimit = None
		self.termInstanceCount = property(self.getTermInstanceCount)
		self.termIdHash = dict()
	
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
			pickle_tools.pickle_dump_attrs(self,self.path,"termInstanceLimit","termIdHash","indexKey")
	
	def zeroAllData(self):
		self.termIdHash = dict()
		# remove disk based data
		if self.path and os.path.exists(self.path):
			os.unlink(self.path)
	
	def getTermInstanceCount(self):
		return sum(map(lambda termInstance: termInstance.termInstanceCount,self.termIdHash.values()))
	
	def reachedTermInstanceLimit(self):
		if self.termInstanceLimit:
			return self.termInstanceLimit == self.termInstanceCount
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

class ExternalPartition(object):
	"""ExternalPartition uses an on disk file to store compressed DocIdTermInstanceTable instances
	In memory it must maintan only enough information to read the proper table for a termId
	This in-memory data must be explicitly preserved to disk, and will be loaded at __init___"""
	__slots__ = ["name","path","indexKey","metadataFileSuffix","termInstanceLimit","termIdHash","termInstanceCount"]
	def __init__(self,_name,_path,_metadataFileSuffix=DefaultMetadataFileSuffix,_indexKey=None):
		self.name = _name
		self.path = _path
		self.indexKey = _indexKey
		self.metadataFileSuffix = _metadataFileSuffix
		self.termInstanceLimit = None
		self.termInstanceCount = property(self.getTermInstanceCount)
		self.termIdHash = dict()
	
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
	
	def getTermInstanceCount(self):
		return sum(map(lambda termInstanceHeader: termInstanceHeader.termInstanceCount,self.termIdHash.values()))
	
	def reachedTermInstanceLimit(self):
		if self.termInstanceLimit:
			return self.termInstanceLimit == self.termInstanceCount
		return False
	
	def lookupTermId(self,termId):
		if termId in self.termIdHash:
			return data.decompressDocIdTermInstanceTable(self.mmap,self.termIdHash[termId])
		return data.nullUncompressedDocIdTermInstanceTable()
	
	def deleteTermId(self,termId):
		"""does not remove data from index, just drops the reference in the termIdHash to prevent lookup"""
		if termId in self.termIdHash: del term.termIdHash[termId]
	
	def estimateSizeOnDisk(self):
		return sum(map(lambda header: header.length,self.termIdHash.values()))
	
	def compressTermIdData(self,termId):
		header = self.termIdHash[termId]
		return data.readCompressedDocIdTermInstanceTable(self.mmap,header)
	
	def __mmap_init__(self):
		"""calling this opens the index file and mmaps it into memory
		the attributes "fp" and "mmap" do not exist before calling this
		it must be called after every index file size change
		This will fail is self.path is missing
		"""
		fileSize = os.stat(self.path).st_size
		self.fp = open(self.path,"rb")
		self.mmap = mmap.mmap(self.fp.fileno(),fileSize,mmap.MAP_SHARED,mmap.PROT_READ)
	
	def mergePartitions(self,termIdList,*partitions):
		"""Merge the data from partitions into self
		This must be a method on an ExternalPartition, MemoryPartitions have no concept of merging
		termIdList must contain the sorted list of all termIds in all partitions
		the resulting merged partition will contain one entry for each termId
		"""
		# seek constants... no need to import them from wherever...
		SEEK_END = 0
		SEEK_CUR = 2
		# internal functions to handle some preliminaries
		# makes the main merge code easier to understand, sacrificing overval function length
		def _growPartitionFile(howMuch):
			"""extend the disk partition by howMuch"""
			if not os.path.exists(self.path):
				print >> sys.stderr, "Creating ExternalPartiton %s" % self.path
				fp = open(self.path,"wb")
			else: # prevent truncation on existing file
				print >> sys.stderr, "Extending ExternalPartition %s" % self.path
				fp = open(self.path,"rb+")

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
				header,compressedData = partitionsHoldingTermId[0].compressTermIdData(termId)
				wp.write(compressedData)
				header.offset = newOffset
				self.termIdHash[termId] = header

				if partitionsHoldingTermId[0] is not self: partitionsHoldingTermId[0].deleteTermId(termId)
			else:
				table = data.DocIdTermInstanceTable()
				for partition in partitions:
					docId,termInstances = partition.lookupTermId(termId)
					for termInstance in termInstances:
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
