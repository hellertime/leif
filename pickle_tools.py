import cPickle as pickle

def _error_free_getattr(obj,attr):
	try:
		return getattr(obj,attr)
	except:
		return None

def _error_free_setattr(obj,attr,attr_value):
	try:
		setattr(obj,attr,attr_value)
	except:
		pass

def pickle_dump_attrs(obj,where,*attrs):
		pickle_data = dict([(attr,attr_val) for attr,attr_val in map(lambda attr: (attr,_error_free_getattr(self,attr)),attrs) if attr_val is not None])
		pickle.dump(pickle_data,open(where,"wb"),-1)

def pickle_load_attrs(obj,where):
	pickle_data = pickle.load(open(where))

	for attr,attr_value in pickle_data.iteritems():
		_error_free_setattr(self,attr,attr_value)
