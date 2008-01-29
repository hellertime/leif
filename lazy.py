"""
Functions and object that act as suspended computatitions.

Currently this is an incomplete implementation, inorder for
this to truly be lazy all inputs and all outputs must also
conform to the lazy protocol.
"""
import collections
import itertools
import sys

class peekable:
	"""An iterator that supports a peek operation.

	Original located at:
	http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/304373

	Example Usage:
	>>> p = peekable(range(4))
	>>> p.peek()
	0
	>>> p.next(1)
	[0]
	>>> p.peek(3)
	[1, 2, 3]
	>>> p.next(2)
	[1, 2]
	>>> p.peek(2)
	Traceback (most recent call last):
	...
	StopIteration
	>>> p.peek(1)
	[3]
	>>> p.next(2)
	Traceback (most recent call last):
	...
	StopIteration
	>>> p.next()
	3
	"""
	def __init__(self, iterable):
		self._iterable = iter(iterable)
		self._cache = collections.deque()
	
	def __iter__(self):
		return self
	
	def _fillcache(self, n):
		while len(self._cache) < n:
			self._cache.append(self._iterable.next())
	
	def next(self, n=None):
		self._fillcache(n is None and 1 or n)
		if n is None:
			result = self._cache.popleft()
		else:
			result = [self._cache.popleft() for i in xrange(n)]
		return result
	
	def peek(self, n=None):
		self._fillcache(n is None and 1 or n)
		if n is None:
			result = self._cache[0]
		else:
			result = [self._cache[i] for i in xrange(n)]
		return result

def minmax(iterable):
	"""find the min and max of iterable, return (min,max)"""
	_min = None
	_max = None
	for i in iter(iterable):
		min_i = min([i])
		max_i = max([i])
		if not _min or min_i < _min: _min = min_i
		if not _max or max_i > _max: _max = max_i
	
	return (_min,_max)

def pairup(l):
	"""take a list of length 2*N and return a list of N lists each of length 2

	returns a generator which will produce the list

	>>> list(pairup([1,2,3,4]))
	[[1,2],[3,4]]
	"""
	izip = itertools.izip
	islice = itertools.islice
	return izip(islice(l,0,len(l)-1,2),islice(l,1,len(l),2))

def flatten(l,depth=None):
	"""flatten a nested list

	returns a generator which will produce the list
	can limit the depth of the flattening by passing a second parameter

	>>> list(flatten([1,2,[3]]))
	[1,2,3]
	>>> list(flatten([1,(2,3),[4,(5,6)]]))
	[1,2,3,4,5,6]
	"""
	if depth is not None and depth <= 0:
		yield l
	else:
		try:
			for e in l:
				if type(e) in (type(()),type([])):
					if depth is not None:
						depth -= 1
					for n in flatten(e,depth):
						yield n
				else:
					yield e
		except TypeError:
			yield l

def predicated_cartesian_product(predicate,first,*rest):
	"""produce all combinations of list elements which satisfy the predicate

	returns a generator which will produce the list

	>>> list(predicated_cartesian_product(None,(0,2),(1,4)))
	[[0,1],[0,4],[2,1],[2,4]]
	>>> def ordered_before(*items):
	...   # define an ordering for simple lists
	...   has_order = True
	...   check,rest = items[0],items[1:]
	...   for item in rest:
	...     if check > item:
	...       has_order = False
	...       break
	...     check = item
	...   return has_order
	>>> list(predicated_cartesian_product(ordered_before,(0,2),(1,4)))
	[[0,1],[0,4],[2,4]]
	"""
	if len(rest) == 0:
		for item in first:
			item = [item]
			if predicate is None or predicate(item):
				yield item
	else:
		for item in first:
			for items in predicated_cartesian_product(None,*rest):
				items = [item] + items
				if predicate is None or predicate(items):
					yield items

def nary_subset(S,n):
	"""produce all subsets of set S that are cardinality n

	returns a generator which will produce the list

	>>> nary_subset([[0,1],[0,4],[2,4]],2)
	[[[0,1],[0,4]],[[0,1],[2,4]],[[0,4],[2,4]]]
	"""
	if n <= 0:
		yield [[]]
	elif not S:
		yield []
	elif n == 1:
		for s in S:
			yield [s]
	else:
		chain = itertools.chain
		imap = itertools.imap

		s0,ss = S[0],S[1:]
		if ss:
			for s in chain(imap(lambda s: [s0] + s,nary_subset(ss,n-1)),nary_subset(ss,n)):
				yield s
