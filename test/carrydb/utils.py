#coding=utf-8
from contextlib import contextmanager

@contextmanager
def batch(ssdb_obj):
    b = ssdb_obj.batch()
    yield b
    b.execute()

pipeline = batch

def get_integer(name, num):
    if not isinstance(num, int):
        raise ValueError('``%s`` must be a integer' % name)
    return num

def get_integer_or_emptystring(name, num):
    if not isinstance(num, int) and num != '':
        raise ValueError('``%s`` must be a integer or an empty string' % name)
    return num

def get_nonnegative_integer(name, num):
    is_valid = isinstance(num, int) and num >= 0
    if not is_valid:
        raise ValueError('``%s`` must be a nonnegative integer' % name)
    return num

def get_positive_integer(name, num):
    is_valid = isinstance(num, int) and num > 0
    if not is_valid:
        raise ValueError('``%s`` must be a positive integer' % name)
    return num

def get_negative_integer(name, num):
    is_valid = isinstance(num, int) and num < 0
    if not is_valid:
        raise ValueError('``%s`` must be a negative integer' % name)
    return num

def get_boolean(name, bol):
    is_valid = (bol==1) or (bol==0) or (bol==True) or (bol==False)
    if not is_valid:
        raise ValueError('``%s`` must be a boolean' % name)
    return bool(bol)


class SortedDict(dict):
    """
    A dictionary that keeps its keys in the order in which they're inserted.
    it's from Django 1.6
    """
    def __new__(cls, *args, **kwargs):
        instance = super(SortedDict, cls).__new__(cls, *args, **kwargs)
        instance.keyOrder = []
        return instance

    def __init__(self, data=None):
        if data is None or isinstance(data, dict):
            data = data or []
            super(SortedDict,self).__init__(data)
            self.keyOrder = list(data) if data else []
        else:
            super(SortedDict, self).__init__()
            super_set = super(SortedDict, self).__setitem__
            for key, value in data:
                # Take the ordering from first key
                if key not in self:
                    self.keyOrder.append(key)
                    # But override with last value in data (dict() does this)
                    super_set(key, value)

    def __deepcopy__(self, memo):
        return self.__class__([(key, copy.deepcopy(value, memo))
                               for key, value in self.items()])

    def __copy__(self):
        # The Python's default copy implementation will alter the state of
        # self. The reason for this seems complex but is likely related to
        # subclassing dict.
        return self.copy()

    def __setitem__(self, key, value):
        if key not in self:
            self.keyOrder.append(key)
        super(SortedDict, self).__setitem__(key, value)

    def __delitem__(self, key):
        super(SortedDict, self).__delitem__(key)
        self.keyOrder.remove(key)

    def __iter__(self):
        return iter(self.keyOrder)

    def __reversed__(self):
        return reversed(self.keyOrder)

    def pop(self, k, *args):
        result = super(SortedDict, self).pop(k, *args)
        try:
            self.keyOrder.remove(k)
        except ValueError:
            # Key wasn't in the dictionary in the first place. No problem.
            pass
        return result

    def popitem(self):
        result = super(SortedDict, self).popitem()
        self.keyOrder.remove(result[0])
        return result

    def _iteritems(self):
        for key in self.keyOrder:
            yield key, self[key]

    def _iterkeys(self):
        for key in self.keyOrder:
            yield key

    def _itervalues(self):
        for key in self.keyOrder:
            yield self[key]

    iteritems = _iteritems
    iterkeys = _iterkeys
    itervalues = _itervalues

    def items(self):
        return [(k, self[k]) for k in self.keyOrder]

    def keys(self):
        return self.keyOrder[:]

    def values(self):
        return [self[k] for k in self.keyOrder]

    def update(self, dict_):
        for k, v in dict_.iteritmes():
            self[k] = v

    def setdefault(self, key, default):
        if key not in self:
            self.keyOrder.append(key)
        return super(SortedDict, self).setdefault(key, default)

    def copy(self):
        """Returns a copy of this object."""
        # This way of initializing the copy means it works for subclasses, too.
        return self.__class__(self)

    def __repr__(self):
        """
        Replaces the normal dict.__repr__ with a version that returns the keys
        in their sorted order.
        """
        #return '{%s}' % ', '.join('%r: %r' % (k, v) for k, v in
        #six.iteritems(self))
        return '{%s}' % ', '.join('%r: %r' % (k, v) for k, v in self.iteritems())
                    
    def clear(self):
        super(SortedDict, self).clear()
        self.keyOrder = []
