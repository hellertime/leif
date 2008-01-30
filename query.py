"""Functions for querying the Index using a s-expr type syntax"""
import compiler.ast
import data
import sys

def getExpressionTreeFromString(inputString):
	ast = compiler.parse(inputString)
	if isinstance(ast,compiler.ast.Module):
		docString,statement = ast
		for node in statement.nodes:
			# Discard is the expression type in compiler
			if isinstance(node,compiler.ast.Discard):
				return node.expr
	
	return None

def makeInitialEnvironmentFromLookupFunction(lookupFunction):
	"""lookupFunction takes a termWord as input and returns a [DocIdTermInstanceVector]"""
	class EnvironmentBase(object):
		__slots__ = ["lookupFunction"]
		def __init__(self,_lookupFunction): self.lookupFunction = _lookupFunction 
		def __contains__(self,termWord): return True
		def __getitem__(self,termWord): return self.lookupFunction(termWord)
	
	return [EnvironmentBase(lookupFunction)]

def reduceTopLevel(expressionTree,initialEnvironment):
	"""initialEnvironment must be a list"""
	if isinstance(expressionTree,compiler.ast.Tuple):
		rator,rands = expressionTree.nodes[0],expressionTree.nodes[1:]
		if isinstance(rator,compiler.ast.Name):
			rator = rator.name
			# Dispatch on rator
			if rator == "Term":
				print >> sys.stderr, "reduce -> Term"
				return reduceTerm(rands,initialEnvironment)
			elif rator == "And":
				print >> sys.stderr, "reduce -> And"
				return reduceAndOp(rands,initialEnvironment)
			elif rator == "Andnot":
				print >> sys.stderr, "reduce -> Andnot"
				return reduceAndnotOp(rands,initialEnvironment)
			elif rator == "Before":
				print >> sys.stderr, "reduce -> Before"
				return reduceBeforeOp(rands,initialEnvironment)
			elif rator == "After":
				print >> sys.stderr, "reduce -> After"
				return reduceAfterOp(rands,initialEnvironment)
			elif rator == "Minoc":
				print >> sys.stderr, "reduce -> Minoc"
				return reduceMinocOp(rands,initialEnvironment)
			elif rator == "Within":
				print >> sys.stderr, "reduce -> Within"
				return reduceWithinOp(rands,initialEnvironment)
			elif rator == "Scope":
				print >> sys.stderr, "reduce -> Scope"
				return reduceScopeOp(rands,initialEnvironment)
	
	return None

def reduceTerm(termConstant,environmentFrames):
	termConstant = termConstant[0]
	if isinstance(termConstant,compiler.ast.Const):
		term = termConstant.value
		for environmentFrame in environmentFrames:
			if term in environmentFrame: return environmentFrame[term]
	
	return None

def reduceSimpleOperator(opcode,expressionTree,environmentFrames):
	op = data.computedMatchVectorOp(opcode)
	return op(*[reduceTopLevel(expression,environmentFrames) for expression in expressionTree])

def reduceAndOp(expressionTree,environmentFrames): return reduceSimpleOperator(data.OP_AND,expressionTree,environmentFrames)
def reduceAndnotOp(expressionTree,environmentFrames): return reduceSimpleOperator(data.OP_ANDNOT,expressionTree,environmentFrames)
def reduceBeforeOp(expressionTree,environmentFrames): return reduceSimpleOperator(data.OP_BEFORE,expressionTree,environmentFrames)
def reduceAfterOp(expressionTree,environmentFrames): return reduceSimpleOperator(data.OP_AFTER,expressionTree,environmentFrames)

def reduceMinocOp(expressionTree,environmentFrames):
	count,expressionTree = expressionTree[0],expressionTree[1:]
	if isinstance(count,compiler.ast.Const):
		count = count.value
		op = data.computedMatchVectorOp(data.OP_MINOC)
		return op(count,*[reduceTopLevel(expression,environmentFrames) for expression in expressionTree])
	
	return None

def reduceWithinOp(expressionTree,environmentFrames):
	count,expressionTree = expressionTree[0],expressionTree[1:]
	if isinstance(count,compiler.ast.Const):
		count = count.value
		op = data.computedMatchVectorOp(data.OP_WITHIN)
		return op(count,*[reduceTopLevel(expression,environmentFrames) for expression in expressionTree])
	
	return None

def reduceScopeOp(expressionTree,environmentFrames):
	if len(expressionTree) > 2: raise ValueError("Scope operator takes exactly two arguments")
	scope,scoped = expressionTree[0],expressionTree[1]
	scopeRator,scopeRand = scope.nodes[0],scope.nodes[1:]
	scopedRator,scopedRand = scoped.nodes[0],scoped.nodes[1:]
	if not (scopeRator.name == scopedRator.name == "Term"): raise ValueError("Scope arguments must be Terms")
	op = data.computedMatchVectorOp(data.OP_SCOPE)
	return op(reduceTerm(scopeRand,environmentFrames),reduceTerm(scopedRand,environmentFrames))
