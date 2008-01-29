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

def reduceTopLevel(expressionTree,initialEnvironment):
	"""initialEnvironment must be a list"""
	if isinstance(expressionTree,compiler.ast.Tuple):
		rator,rands = expressionTree.nodes[0],expressionTree.nodes[1:]
		if isinstance(rator,compiler.ast.Name):
			rator = rator.name
			# Dispatch on rator
			if rator == "Term":
				return reduceTerm(rands,initialEnvironment)
			elif rator == "And":
				return reduceAndOp(rands,initialEnvironment)
			elif rator == "Andnot":
				return reduceAndnotOp(rands,initialEnvironment)
	
	return None

def reduceTerm(termConstant,environmentFrames):
	termConstant = termConstant[0]
	if isinstance(termConstance,compiler.ast.Const):
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
	scopeRator,scopeRand = scope.nodes[0],scope.nodes[1]
	scopedRator,scopedRand = scoped.nodes[0],scoped.nodes[1]
	if not (scopeRator.name == scopedRator.name == "Term"): raise ValueError("Scope arguments must be Terms")
	op = data.computedMatchVecorOp(data.OP_SCOPE)
	return op(reduceTerm(scopeRand,environmentFrames),reduceTerm(scopedRand,environmentFrames))
