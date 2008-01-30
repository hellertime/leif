"""
Classes and functions for dealing with data associated to the Indexer
"""
import itertools
import lazy
import operator
import struct
import sys

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
	def __eq__(self,termInstance): return self.position == termInstance.position
	def __gt__(self,termInstance): return self.position > termInstance.position
	def __lt__(self,termInstance): return self.position < termInstance.position
	def __ge__(self,termInstance): return self.position >= termInstance.position
	def __le__(self,termInstance): return self.position <= termInstance.position

class DocIdTermInstanceTable(object):
	"""In the Index each TermId will have a pointer to a DocIdTermTable
	A DocIdTermTable has the following data:
	docIdHash (DocId :: Int, TermInstanceSet :: Set)
	termInstanceCount :: Int"""
	__slots__ = ["docIdHash"]
	def __init__(self):
		self.docIdHash = dict()
	
	@property
	def termInstanceCount(self):
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

class DocIdTermInstanceVector(object):
	"""Replaces the tuple returned by readers so that flatten will not expand the docId,[TermInstances] pairing"""
	__slots__ = ["docId","termInstancesGenerator"]
	def __init__(self,_docId,_termInstancesGenerator):
		self.docId = _docId
		self.termInstancesGenerator = _termInstancesGenerator
	
	def __eq__(self,docIdTermInstanceVector): return self.docId == docIdTermInstanceVector.docId
	def __gt__(self,docIdTermInstanceVector): return self.docId > docIdTermInstanceVector.docId
	def __lt__(self,docIdTermInstanceVector): return self.docId < docIdTermInstanceVector.docId
	def __ge__(self,docIdTermInstanceVector): return self.docId >= docIdTermInstanceVector.docId
	def __le__(self,docIdTermInstanceVector): return self.docId <= docIdTermInstanceVector.docId

	def __repr__(self): return "#TIV:%d<%s>" % (self.docId,list(self.termInstancesGenerator))
	
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
		skipOffsetBytes = DocIdSizeInBytes
		termInstanceCount = len(termInstances) # note this is not the same as: _table.termInstanceCount
		skipOffsetBytes += termInstanceCount * TermInstanceSizeInBytes
		termInstanceVector = list(lazy.flatten(map(_tuple,sorted(termInstances))))
		termInstanceBlocks.append(_pack("!II%dI" % len(termInstanceVector),skipOffsetBytes,docId,*termInstanceVector))
	
	compressedData = "".join(termInstanceBlocks)
	header = CompressedDocIdTermInstanceTableHeader()
	header.docIdCount = len(_table)
	header.termInstanceCount = _table.termInstanceCount
	header.length = len(compressedData)
	return (header,compressedData)

def decompressDocIdTermInstanceTable(_buffer,_header):
	"""Creates a Python generator which will produce (docId,[TermInstance]) tuple
	the [TermInstance] is a generator which will produce the TermInstance structures
	associated with docId"""
	def generateDocIdTermInstanceVectors(_offset,_length):
		currentOffset = _offset
		_unpack = struct.unpack
		while currentOffset < _offset + _length:
			headerBytes = _buffer[currentOffset:currentOffset+SkipOffsetSizeInBytes+DocIdSizeInBytes]
			skipOffset,docId = _unpack("!II",headerBytes)
			currentOffset += SkipOffsetSizeInBytes+DocIdSizeInBytes
			# NOTE: the use of PositionSizeInBytes only works because PositionSizeInBytes == ExtentSizeInBytes
			termInstanceElementCount = (skipOffset - DocIdSizeInBytes) / PositionSizeInBytes
			termInstanceBytes = _buffer[currentOffset:currentOffset+(termInstanceElementCount*PositionSizeInBytes)]
			currentOffset += termInstanceElementCount*PositionSizeInBytes
			termInstanceElements = _unpack("!%dI" % termInstanceElementCount,termInstanceBytes)
			def termInstanceGenerator(termInstanceElements):
				for _position,_extent in lazy.pairup(termInstanceElements):
					termInstance = TermInstance(_position,_extent)
					yield termInstance

			yield DocIdTermInstanceVector(docId,termInstanceGenerator(termInstanceElements))

	return generateDocIdTermInstanceVectors(_header.offset,_header.length)

def readCompressedDocIdTermInstanceTable(_buffer,_header):
	return (_header,_buffer[_header.offset:_header.offset+_header.length])

def readUncompressedDocIdTermInstanceTable(_table):
	"""Creates a Python generator which will produce (docId,[TermInstance]) tuple
	see decompressDocIdTermInstanceTable"""
	def generateDocIdTermInstanceVectors():
		for docId in sorted(_table.docIdHash):
			def termInstanceGenerator(_table,docId):
				for termInstance in sorted(_table.docIdHash[docId]):
					yield termInstance

			yield DocIdTermInstanceVector(docId,termInstanceGenerator(_table,docId))
	
	return generateDocIdTermInstanceVectors()

def nullUncompressedDocIdTermInstanceTable():
	# returns a generator object to match semantics of a reader
	return (_ for _ in [(None,iter([]))])

def joinUncompressedDocIdTermInstanceTableReaders(readerList):
	_peekable = lazy.peekable
	_ifilter = itertools.ifilter
	_flatten = lazy.flatten
	return _peekable(sorted(_ifilter(None,_flatten(map(None,*readerList)))))

class AnalyzedTerm(object):
	"""each term is really a set of term occurrences"""
	__slots__ = ["instanceSet"]
	def __init__(self):
		self.instanceSet = set()
	
	def addTermIdWithOptionalExtent(self,termId,extent=0):
		self.instanceSet.add((termId,extent))
	
	def __repr__(self): return "#AT:%s" % repr(self.instanceSet)

class AnalyzedDocument(object):
	"""An AnalyzedDocument is a docId,[AnalyzedTerm] structure"""
	__slots__ = ["docId","analyzedTermList"]
	def __init__(self,_docId):
		self.docId = _docId
		self.analyzedTermList = list()
	
	def appendAnalyzedTerm(self,analyzedTerm):
		self.analyzedTermList.append(analyzedTerm)
	
	def __repr__(self): return "#AD(%d):%s" % (self.docId,repr(self.analyzedTermList))

class ComputedMatch(object):
	"""Queries produce ComputedMatch(es)"""
	__slots__ = ["docId","termInstanceVectors"]
	def __init__(self,_docId,_termInstanceVectors):
		self.docId = _docId
		self.termInstanceVectors = _termInstanceVectors
	
	def computedMatchCartesianProductWithPredicate(self,predicate):
		"""Return a new ComputedMatch which is the cartesian product of self.termInstanceVectors"""
		return ComputedMatch(self.docId,list(lazy.predicated_cartesian_product(predicate,*self.termInstanceVectors)))
	
	def computedMatchSubsets(self,minSubsetSize=1,maxSubsetSize=5):
		"""Return a new ComputedMatch with termInstanceVectors containing all the sub-sets of self.termInstaceVectors
		Sub-sets will contain minSubsetSize to maxSubsetSize members
		If maxSubsetSize is None and minSubsetSize=1 then this will effectively compute the powerset of self
		*The POWERSET can be HUGE, be careful*"""
		powerset = list()
		minSubsetSize = minSubsetSize or 1
		maxSubsetSize = maxSubsetSize or sys.maxint
		if maxSubsetSize < minSubsetSize: maxSubsetSize = minSubsetSize + 1
		for subsetSize in xrange(minSubsetSize,maxSubsetSize + 1):
			try:
				powerset += list(lazy.nary_subset(self.termInstanceVectors,subsetSize))
			except StopIteration:
				break

		return ComputedMatch(self.docId,powerset)
	
	def _addOpAssert(self,other):
		if not isinstance(other,ComputedMatch): raise TypeError("Cannot add ComputedMatch and %s" % type(other))
		if self.docId != other.docId: raise ValueError("Cannot add ComputedMatch objects of differing docId")
	
	def __add__(self,computedMatch):
		"""Return a new ComputedMatch"""
		self._addOpAssert(computedMatch)
		return ComputedMatch(self.docId,[self.termInstanceVectors,computedMatch.termInstanceVectors])
	# make it work in both directions
	__radd__ = __add__

	def __iadd__(self,computedMatch):
		"""Concatenate termInstanceVectors"""
		self._addOpAssert(computedMatch)
		self.termInstanceVectors += computedMatch.termInstanceVectors
		self.termInstanceVectors.sort()
		return self # Should __iadd__ return a value?
	
	def __getitem__(self,index): return self.termInstanceVectors[index]
	def __len__(self): return len(self.termInstanceVectors)
	def __hash__(self): return int(self.docId)
	def __eq__(self,computedMatch): self.docId == computedMatch.docId
	def __ne__(self,computedMatch): self.docId != computedMatch.docId
	def __gt__(self,computedMatch): self.docId > computedMatch.docId
	def __lt__(self,computedMatch): self.docId < computedMatch.docId
	def __ge__(self,computedMatch): self.docId >= computedMatch.docId
	def __le__(self,computedMatch): self.docId <= computedMatch.docId
	def __iter__(self): return iter(self.termInstanceVectors)
	def __repr__(self): return "#CM(%d):%s" % (self.docId,repr(self.termInstanceVectors))

class ComputedMatchVector(object):
	"""A container-like object holding ComputedMatch(es)
	This does not provide a len(), since the internal generator can be infinite"""
	__slots__ = ["computedMatchGenerator","realizedComputedMatchVector"]
	def __init__(self,_computedMatchGenerator):
		self.computedMatchGenerator = _computedMatchGenerator
		self.realizedComputedMatchVector = list()
	
	def __iter__(self):
		def vectorIterator(computedMatchVector):
			iterationIndex = 0
			while 1:
				try:
					yield computedMatchVector[iterationIndex]
				except IndexError:
					raise StopIteration
				iterationIndex += 1

		return vectorIterator(self)
	
	def __getitem__(self,index):
		try:
			while len(self.realizedComputedMatchVector) < index + 1:
				self.realizedComputedMatchVector.append(self.computedMatchGenerator.next())
		except StopIteration:
			pass
		return self.realizedComputedMatchVector[index]
	
	def __repr__(self): return "#CMV:%s..." % (repr(self.realizedComputedMatchVector))

def computedMatchVectorAndOp(*computedMatchVectors):
	"""Return a new ComputedMatchVector where all ComputedMatch(es) has equal docId(s)"""
	def computedMatchGenerator():
		docIdEq = lambda computedMatch: len(frozenset(computedMatch)) == 1
		for computedMatches in lazy.predicated_cartesian_product(docIdEq,*computedMatchVectors):
			if 0 not in map(len,computedMatches):
				yield reduce(operator.__add__,computedMatches)
	
	return ComputedMatchVector(computedMatchGenerator())

def computedMatchVectorAndnotOp(*computedMatchVectors):
	"""Return a new ComputedMatchVector iff the first computedMatchVector has output and all others have no output"""
	def computedMatchGenerator():
		docIdEq = lambda computedMatch: len(frozenset(computedMatch)) == 1
		for computedMatches in lazy.predicated_cartesian_product(docIdEq,*computedMatchVectors):
			if len(computedMatches) == 1 or (len(computedMatches) > 1 and 0 == sum(map(len,computedMatches[1:]))):
				yield computedMatches[0]
	
	return ComputedMatchVector(computedMatchGenerator())

def _min(it):
	m = min(it)
	print >> sys.stderr, "DebugMin: %s -> %s" % (repr(it),repr(m))
	return m

def _max(it):
	m = max(it)
	print >> sys.stderr, "DebugMax: %s -> %s" % (repr(it),repr(m))
	return m

def computedMatchVectorBeforeOp(*computedMatchVectors):
	"""Return a new ComputedMatchVector where the ComputedMatches are ordered ascending by instance position"""
	def ascendingOrderTest(computedMatches):
		if not computedMatches: return False
		_flatten = lazy.flatten
		inOrder = True
		testComputedMatch = None
		for checkComputedMatch in computedMatches:
			if testComputedMatch and max(list(_flatten(testComputedMatch))).position > min(list(_flatten(checkComputedMatch))).position:
				inOrder = False
				break
			testComputedMatch = checkComputedMatch

		return inOrder
	
	def computedMatchGenerator():
		for computedMatch in computedMatchVectorAndOp(*computedMatchVectors):
			computedMatch = computedMatch.computedMatchCartesianProductWithPredicate(ascendingOrderTest)
			if len(computedMatch):
				yield computedMatch
	
	return ComputedMatchVector(computedMatchGenerator())

def computedMatchVectorAfterOp(*computedMatchVectors):
	"""Return a new ComputedMatchVector where the ComputedMatches are ordered descending by instance position"""
	def descendingOrderTest(computedMatches):
		if not computedMatches: return False
		_flatten = lazy.flatten
		inOrder = True
		testComputedMatch = None
		for checkComputedMatch in computedMatches:
			if testComputedMatch and max(list(_flatten(testComputedMatch))).position < min(list(_flatten(checkComputedMatch))).position:
				inOrder = False
				break
			testComputedMatch = checkComputedMatch

		return inOrder
	
	def computedMatchGenerator():
		for computedMatch in computedMatchVectorAndOp(*computedMatchVectors):
			computedMatch = computedMatch.computedMatchCartesianProductWithPredicate(descendingOrderTest)
			if len(computedMatch):
				yield computedMatch
	
	return ComputedMatchVector(computedMatchGenerator())

def computedMatchVectorWithinOp(distanceConstraint,*computedMatchVectors):
	"""Return a new ComputedMatchVector where the ComputedMatches are Within distanceContraint of each others position"""
	def distanceTest(computedMatches):
		if not computedMatches: return False
		_flatten = lazy.flatten
		for computedMatchPair in lazy.nary_subset(list(_flatten(computedMatches)),2):
			if abs(computedMatchPair[0].position - computedMatchPair[1].position) <= distanceConstraint:
				return True
	
	def computedMatchGenerator():
		for computedMatch in computedMatchVectorAndOp(*computedMatchVectors):
			if distanceTest(computedMatch):
				yield computedMatch
	
	return ComputedMatch(computedMatchGenerator())

def computedMatchVectorMinocOp(minOccurrence=1,*computedMatchVectors):
	def computedMatchGenerator():
		for computedMatch in computedMatchVectorAndOp(*computedMatchVectors):
			computedMatch = computedMatch.computedMatchSubsets(minOccurrence)
			for subset in computedMatch:
				if len(subset) == minOccurrence: # test that at least one subset has the minOccurrence length
					yield computedMatch
					break
	
	return ComputedMatch(computedMatchGenerator())

def computedMatchVectorScopeOp(scopeComputedMatchVector,scopedComputedMatchVector):
	"""Returns a ComputedMatchVector when the scoped* position is covered by the scope* extent"""
	def scopeTest(computedMatches):
		if not computedMatches: return False
		scope,scoped = computedMatches
		if scope.position == scoped.position or (scope.position < scoped.position and scoped.position < (scoped.position + scope.extent)):
			return True
	
	def computedMatchGenerator():
		for computedMatch in computedMatchVectorAndOp(scopeComputedMatchVector,scopedComputedMatchVector):
			computedMatch = computedMatch.computedMatchCartesianProduceWithPredicate(scopeTest)
			yield ComputedMatch(computedMatch.docId,[computedMatch[0][1]])
	
	return ComputedMatchVector(computedMatchGenerator())

OP_AND = 1
OP_ANDNOT = 2
OP_BEFORE = 3
OP_AFTER = 4
OP_MINOC = 5
OP_WITHIN = 6
OP_SCOPE = 7

def computedMatchVectorOp(opcode):
	if opcode == OP_AND: return computedMatchVectorAndOp
	elif opcode == OP_ANDNOT: return computedMatchVectorAndnotOp
	elif opcode == OP_BEFORE: return computedMatchVectorBeforeOp
	elif opcode == OP_AFTER: return computedMatchVectorAfterOp
	elif opcode == OP_MINOC: return computedMatchVectorMinocOp
	elif opcode == OP_WITHIN: return computedMatchVectorWithinOp
	elif opcode == OP_SCOPE: return computedMatchVectorScopeOp
