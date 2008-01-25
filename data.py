"""
Classes and functions for dealing with data associated to the Indexer
"""
import lazy
import struct

class TermInstance(object):
	"""The Index must deal with TermInstances when satisfying Queries
	A TermInstance is a structure consisting of:
	position :: Int
	extent :: Int

	it is hashable only on the Position data"""
	__slots__ = ["position","extent"]
	def __init__(self,_position,_extent=0):
		self.position = _position
		self.extent = _extent
	
	def __hash__(self): return int(self.position)
	def __repr__(self): return '#TI:%d,%d' % (self.position,self.extent)
	# the following should only be used for ordering not to test integers against the position
	def __gt__(self,termInstance): return self.position > termInstance.position
	def __lt__(self,termInstance): return self.position < termInstance.position
	def __ge__(self,termInstance): return self.position >= termInstance.position
	def __le__(self,termInstance): return self.position <= termInstance.position

class DocIdTermInstanceTable(object):
	"""In the Index each TermId will have a pointer to a DocIdTermTable
	A DocIdTermTable has the following data:
	docIdHash (DocId :: Int, TermInstanceSet :: Set)
	termInstanceCount :: Int"""
	__slots__ = ["docIdHash","termInstanceCount"]
	def __init__(self):
		self.docIdHash = dict()
		self.termInstanceCount = property(self.getTermInstanceCount)
	
	def getTermInstanceCount(self):
		return sum(map(len,self.docIdHash.values()))
	
	def insertTermInstanceRecord(self,docId,termInstance):
		if docId not in self.docIdHash: self.docIdHash[docId] = set()
		self.docIdHash[docId].add(termInstance)
	
	def deleteDocId(self,docId):
		if docId in self.docIdHash: del self.docIdHash[docId]
	
	def __len__(self): return len(self.docIdHash)
	def __contains__(self,docId):	return docId in self.docIdHash
	def __repr__(self): return "<DocIdTermInstanceTable %d docId(s) %d termInstance(s)>" % (len(self),self.termInstanceCount)

class CompressedDocIdTermInstanceTableHeader(object):
	__slots__ = ["offset","length","docIdCount","termInstanceCount"]
	def __init__(self):
		self.offset = 0
		self.length = 0
		self.docIdCount = 0
		self.termInstanceCount = 0

# Disk Layout Constants
SkipOffsetSizeInBytes = 4 # on disk we store the seek offset to the end of a table
DocIdSizeInBytes = 4
PositionSizeInBytes = 4
ExtentSizeInBytes = 4
TermInstanceSizeInBytes = PositionSizeInBytes + ExtentSizeInBytes

def estimateSizeOfDocIdTermInstanceTable(_table):
	"""Calculates the maximal size of the _table
	this size may be larger than the actual size since
	the compressor can be more efficient"""
	bytes = 0
	for docId in _table.docIdHash:
		bytes += SkipOffsetSizeInBytes + DocIdSizeInBytes
		bytes += len(_table.docIdHash[docId]) * TermInstanceSizeInBytes
	
	return bytes

def compressDocIdTermInstanceTable(_table):
	"""Creates a compressed packed byte string of the _table
	returns a tuple the first value is the CompressedDocIdTermInstanceTableHeader for this
	the second is the compressed data itself
	*currently there is no compression*"""
	_pack = struct.pack
	_tuple = lambda termInstance: (termInstance.position,termInstance.extent)
	termInstanceBlocks = []
	for docId in sorted(_table.docIdHash):
		termInstances = _table.docIdHash[docId]
		skipOffsetBytes = docIdSizeInBytes
		termInstanceCount = len(termInstances) # note this is not the same as: _table.termInstanceCount
		skipOffsetBytes += termInstanceCount * TermInstanceSizeInBytes
		termInstanceVector = list(lazy.flatten(map(_tuple,sorted(termInstances))))
		termInstanceBlocks.append(_pack("!II%dI" % len(termInstanceVector),skipOffsetBytes,docId,*termInstanceVector))
	
	compressedData = "".join(termInstanceBlocks)
	header = CompressedDocIdTermInstanceTableHeader()
	header.docIdCount = len(_table)
	header.termIdCount = _table.termIdCount
	header.length = len(compressedData)
	return (header,compressedData)

def decompressDocIdTermInstanceTable(_buffer,_header):
	"""Creates a Python generator which will produce (docId,[TermInstance]) tuple
	the [TermInstance] is a generator which will produce the TermInstance structures
	associated with docId"""
	def generateGenerator(_offset,_length):
		currentOffset = 0
		_unpack = struct.unpack
		while currentOffset < _offset + _length:
			headerBytes = _buffer[_currentOffset:_currentOffset+SkipOffsetSizeInBytes+DocIdSizeInBytes]
			skipOffset,docId = _unpack("!II",headerBytes)
			currentOffset += SkipOffsetSizeInBytes+DocIdSizeInBytes
			# NOTE: the use of PositionSizeInBytes only works because PositionSizeInBytes == ExtentSizeInBytes
			termInstanceElementCount = (skipOffset - DocIdSizeInBytes) / PositionSizeInBytes
			termInstanceBytes = _buffer[currentOffset:currentOffset+(termInstanceElementCount*PositionSizeInBytes)]
			currentOffset += termInstanceElementCount*PositionSizeInBytes
			termInstanceElements = _unpack("!%dI" % termInstanceElementCount,termInstanceBytes)
			def termInstanceGenerator():
				for _position,_extent in lazy.pairup(termInstanceElements):
					yield TermInstance(_position,_extent)

			yield (docId,termInstanceGenerator())

	return generateGenerator(_header.offset,_header.length)

def readCompressedDocIdTermInstanceTable(_buffer,_header):
	return (header,_buffer[_header.offset:_header.offset+_header.length])

def readUncompressedDocIdTermInstanceTable(_table):
	"""Creates a Python generator which will produce (docId,[TermInstance]) tuple
	see decompressDocIdTermInstanceTable"""
	def generatorGenerator():
		for docId in sorted(_table.docIdHash):
			def termInstanceGenerator():
				for termInstance in sorted(_table.docIdHash[docId]):
					yield termInstance

			yield (docId,termInstanceGenerator())
	
	return generatorGenerator()

def nullUncompressedDocIdTermInstanceTable():
	return (None,iter([]))
