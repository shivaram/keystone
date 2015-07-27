from graphops import load_graph, get_preds, get_attr, get_succs
from frank_wolfe import frank_wolfe_constrained_qp
import numpy as np
import sys


def totals(attr, preds, k):
  return attr[k] + sum([totals(attr, preds, j) for j in preds[k]])

def get_attr_totals(graph, attr="loc"):
  """Returns a lookup table of all the local time along paths to each node in a graph."""
  preds = get_preds(graph)
  locs = get_attr(graph, attr)
  return {k:totals(locs,preds,k) for k in graph["vertices"].keys()}


def path_counts(succs, k):
  if k == "sink":
    return 1
  else:
    return sum([path_counts(succs, j) for j in succs[k]])
  
def get_path_counts(graph):
  succs = get_succs(graph)
  return {k:path_counts(succs, k) for k in graph["vertices"].keys()}
  

def pair_counts(preds, i, j):
  if i == j:
    return 1
  return sum([pair_counts(preds, i, k) for k in preds[j]])
  
def get_path_pair_counts(graph):
  vertices = graph["vertices"].keys()
  preds = get_preds(graph)
  
  pairs = {}
  for i in vertices:
    for j in vertices:
      pairs[(i,j)] = pair_counts(preds,i,j)
  return pairs
  
def get_mats(pairs, tots, counts, mem):
  #Produce the sorted offset list - just for easier bookkeeping.
  off = {sorted(mem.keys())[j]:j for j in range(len(mem.keys()))}
  ns = len(off.keys())
  
  b = np.zeros(ns)
  for t in tots:
    b[off[t]] = -1.0*(counts[t] - 1.0)*tots[t]
  
  #TODO: Finish setting up the QP.
  A = np.zeros([ns,ns])
  for (f,t),v in pairs.items():
    print f,t,v
    if v > 0:
      A[off[f],off[t]] = (pairs[f,t] - 1.0)*tots[f]
  
  print off
  print A
  
  c = np.zeros(ns)
  for m in mem:
    c[off[m]] = mem[m]
  
  return A, b, c, off

def main():
  graph = load_graph(sys.stdin)
  tots = get_attr_totals(graph)
  counts = get_path_counts(graph)
  pairs = get_path_pair_counts(graph)
  
  print "Graph: ", graph
  print "Tots: ", tots
  print "Counts: ", counts
  print "Pairs: ", pairs
  #Given pairs, graph, tots, counts, form the A matrix and the b and c vectors.
  mem_req = get_attr(graph, "mem")
    
  A, b, c, lt = get_mats(pairs, tots, counts, mem_req)
  
  ns = len(b)
  w = np.ones(ns)
  dot = np.dot
  print "Allon: ", dot(dot(w.T,A),w) + dot(b,w)
  
  B = float(sys.argv[1]) #Memory budget.
  z = frank_wolfe_constrained_qp(ns, A, b, c, B)
  inv_lt = {v:k for k,v in lt.items()}
  out = {inv_lt[i]:z[i] for i in range(len(z))}
  print out
  
  

if __name__ == "__main__":
  main()