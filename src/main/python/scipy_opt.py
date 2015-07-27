import scipy.optimize
import numpy as np
from functools import partial

from estimate_dag import estimate_dag_time, make_flat_vec, get_attr, make_cache_dict
from graphops import load_graph


class MyBounds(object):
     def __init__(self, maxmem, memreq, xmax=1.0, xmin=0.0):
         self.maxmem = maxmem
         self.memreq = memreq
         self.xmax = xmax
         self.xmin = xmin
     def __call__(self, **kwargs):
         x = kwargs["x_new"]
         tmax = bool(np.all(x <= self.xmax))
         tmin = bool(np.all(x >= self.xmin))
         mem = bool(np.inner(x, self.memreq) <= self.maxmem)
         return tmax and tmin and mem

 
def main():
  #Workflow is this:
  #1: Read a graph from a file.
  import sys
  graph = load_graph(sys.stdin)
    
  #2: Given graph, produce a function that just takes a cache spec 
  #and produces a number. functools.partial to the rescue.
  #Try two graphs - one with nothing and one with everything
  graph_size = len(graph["vertices"].keys())  
  
  print estimate_dag_time(np.zeros(graph_size), graph)
  print estimate_dag_time(np.ones(graph_size), graph)
  print estimate_dag_time(np.array([1.0,1.0,0.0,1.0,1.0]), graph)
  print estimate_dag_time(np.array([1.0,1.0,0.0,0.0,1.0]), graph)
  print estimate_dag_time(np.array([1.0,1.0,0.5,1.0,1.0]), graph)
  print estimate_dag_time(np.array([ 0.54656042,  0.50245485,  0.98462249,  0.15543588,  0.18881177]), graph)

  #Setup inputs for optimization routine.
  minfunc = partial(estimate_dag_time, graph=graph)
  init_guess = np.zeros(graph_size)
  #init_guess = np.ones(graph_size)/2.0 - 0.1
  lower_bound = np.zeros(graph_size)
  upper_bound = np.ones(graph_size)
  
  #Call optimization routine.
  #res = scipy.optimize.anneal(
  #  func=minfunc, 
  #  x0=init_guess,
  #  lower=0.0,
  #  upper=1.0
  #)
  
  memvec = make_flat_vec(get_attr(graph, "mem"), graph)
  
  res = scipy.optimize.basinhopping(
     func=minfunc,
     x0=init_guess, 
     minimizer_kwargs={},
     niter=1000, 
     accept_test=MyBounds(150.0, memvec),
     stepsize=0.1
     )
  
  print res
  print "Cache Settings: ", make_cache_dict(res.x, graph)
  print "Mem consumption: ", np.inner(memvec, res.x)
  print "Time estimate: ", minfunc(res.x)
  

  
if __name__ == "__main__":
  main()