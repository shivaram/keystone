import json

#This file assumes that a graph is a structure like this:
# graph = {"vertices": {k:{attr..}..}, "edges": set((id1,id2)..)}

def get_attr(graph, attr):
  return {v:float(graph["vertices"][v][attr]) for v in graph["vertices"].keys()}

def get_pred(g, i):
  return set([e[0] for e in g["edges"] if e[1] == i])

def get_preds(g):
  return {v:get_pred(g,v) for v in g["vertices"].keys()}

def get_succ(g, i):
  return set([e[1] for e in g["edges"] if e[0] == i])
  
def get_succs(g):
  return {v:get_succ(g,v) for v in g["vertices"].keys()}
  
def load_graph(f):
  return json.loads(f.read())