""" Utilities for generating Myria plans """

from functools import partial
import jsonpath_rw as jsonpath

DEFAULT_SCAN_TYPE = 'FileScan'
DEFAULT_INSERT_TYPE = 'DbInsert'


class MyriaPlan(object):
    expressions = {
        'fragments': jsonpath.parse('$.plan.fragments.[*]'),
        'operators': jsonpath.parse('$.plan.fragments.[*].operators.[*]')
        }

    def __init__(self, json):
        self.json = json

    @property
    def type(self):
        return self.json['plan']['type']

    @property
    def language(self):
        return self.json['language']

    @property
    def profiling_mode(self):
        return self.json['profilingMode']

    @property
    def text(self):
        return self.json['rawQuery']

    @property
    def logicalRa(self):
        return self.json['logicalRa']

    @property
    def fragments(self):
        return (MyriaFragment(f.value, f.path, f.context, self) for f in self.expressions['fragments'].find(self.json))

    @property
    def operators(self):
        return (MyriaOperator(o.value, o.path, o.context, self) for o in self.expressions['operators'].find(self.json))

    def __str__(self):
        return "MyriaPlan(%s)" % (self.text or self.json)

    def __repr__(self):
        return self.__str__()


class MyriaFragment(jsonpath.DatumInContext):
    expressions = {
        'operators': jsonpath.parse('`this`.operators[*]')
        }

    def __init__(self, value, path=None, context=None, plan=None):
        super(MyriaFragment, self).__init__(value, path, context)
        self.plan = plan

    @property
    def workers(self):
        return self.value['overrideWorkers']

    @property
    def operators(self):
        return (MyriaOperator(o.value, o.path, o.context, self.plan) for o in self.expressions['operators'].find(self))


class MyriaOperator(jsonpath.DatumInContext):
    def __init__(self, value, path=None, context=None, plan=None):
        super(MyriaOperator, self).__init__(value, path, context)
        self.plan = plan

    @property
    def id(self):
        return self.value['opId']

    @property
    def name(self):
        return self.value['opName']

    @property
    def type(self):
        return self.value['opType']

    @property
    def fragment(self):
        return self.context.context

    @property
    def parent(self):
        return next((op for op in self.plan.operators
                        for child in op.children
                        if self.id == child.id), None)

    @property
    def children(self):
        return (op for op in self.plan.operators
                   if op.id in self._child_ids)

    @property
    def _child_ids(self):
        # Is this better than explicitly enumerating child attribute names?
        return (value for key, value in self.items()
                      if 'CHILD' in key.upper() and
                         isinstance(value, int))

    def add(self, key, value): self.value.add(key, value)
    def items(self): return self.value.items()
    def keys(self): return self.value.keys()
    def values(self): return self.value.values()
    def __len__(self): return len(self.value)
    def __iter__(self): return self.value.__iter__()
    def __getitem__(self, item): return self.value[item]
    def __setitem__(self, item, value): self.value[item] = value
    def __delitem__(self, key): del self.value[key]
    def __contains__(self, item): return self.value.contains(item)


def get_parallel_import_plan(schema, work, relation, text='',
                             scan_parameters=None, insert_parameters=None,
                             scan_type=None, insert_type=None):
    """ Generate a valid JSON Myria plan for parallel import of data

    work: list of (worker-id, data-source) pairs; data-source should be a
          JSON data source encoding
    relation: dict containing a qualified Myria relation name

    Keyword arguments:
      text: description of the plan
      scan_parameters: dict of additional operator parameters for the scan
      insert_parameters: dict of additional operator parameters for insertion
      scan_type: type of scan to perform
      insert_Type: type of insert to perform
    """
    return \
        {"fragments": map(partial(_get_parallel_import_fragment, [0],
                                  schema, relation,
                                  scan_type, insert_type,
                                  scan_parameters, insert_parameters), work),
         "logicalRa": text,
         "rawQuery": text}


def _get_parallel_import_fragment(taskid, schema, relation,
                                  scan_type, insert_type,
                                  scan_parameters, insert_parameters,
                                  assignment):
    """ Generate a single fragment of the parallel import plan """
    worker_id = assignment[0]
    datasource = assignment[1]

    scan = {
        'opId': __increment(taskid),
        'opType': scan_type or DEFAULT_SCAN_TYPE,

        'schema': schema.to_dict(),
        'source': datasource
    }
    scan.update(scan_parameters or {})

    insert = {
        'opId': __increment(taskid),
        'opType': insert_type or DEFAULT_INSERT_TYPE,

        'argChild': taskid[0] - 2,
        'argOverwriteTable': True,

        'relationKey': relation
    }
    insert.update(insert_parameters or {})

    return {'overrideWorkers': [worker_id],
            'operators': [scan, insert]}


def __increment(value):
    value[0] += 1
    return value[0] - 1
