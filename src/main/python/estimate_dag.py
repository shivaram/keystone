from functools import partial
from memoize import memoize
from graphops import get_preds, get_succs, get_attr
import numpy as np
import scipy.optimize


#@memoize
def tot(cache, i, r, loc, preds):
  if bool(np.any(np.array(cache.values()) < 0.0)) or bool(np.any(np.array(cache.values()) > 1.0)):
    return 1e6
  return (1.0 - cache[i]) * \
    (loc[i] + sum([tot(cache, j, r, loc, preds) for j in preds[i]])) + \
    cache[i] * \
    (loc[i] + sum([tot(cache, j, r, loc, preds) for j in preds[i]])) / max(r[i], 1.0)
    
def tot_n(cache, i, r, loc, preds):
  if cache[i] <= 0.5:
    return (loc[i] + sum([tot_n(cache, j, r, loc, preds) for j in preds[i]]))
  else:
    return (loc[i] + sum([tot_n(cache, j, r, loc, preds) for j in preds[i]])) / max(r[i], 1.0)


def r(i, succ, cache):
  if len(succ[i]) < 1:
    return 1.0
  else:
    return sum([(1.0-round(cache[j]))*(r(j, succ, cache) - 1.0) + 1.0 for j in succ[i]])


def get_r(cache, graph):
  succ = get_succs(graph)
  return {v:r(v,succ,cache) for v in graph["vertices"].keys()}


def make_cache_dict(cachev, graph):
  return {graph["vertices"].keys()[i]:cachev[i] for i in xrange(len(graph["vertices"].keys()))}


def make_flat_vec(tbl, graph):
  return np.array([tbl[i] for i in graph["vertices"].keys()])


def estimate_dag_time(cache, graph, tot_fun=tot):
  cache = make_cache_dict(cache, graph)
  #print cache
  r = get_r(cache, graph)
  #print r
  loc = get_attr(graph, "loc")
  preds = get_preds(graph)  
  return tot_fun(cache, 'sink', r, loc, preds)


