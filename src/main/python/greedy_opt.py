from estimate_dag import get_r, tot, estimate_dag_time
from graphops import load_graph, get_attr, get_preds


def cache_mem(mem, cache):
  return sum([mem[k]*cache[k] for k in mem.keys()])


def still_room(cache, runs, mem, space):
  bigruns = set([k for k in runs if runs[k] > 1.0])
  uncached = set([k for k in cache if cache[k] < 1.0])
  if len(uncached - bigruns) == 0:
    return False
  return any([runs[k] > 1.0 and mem[k] < space for k in mem.keys()])


def select_next(graph, cache, runs, memleft):
  #Candidate set: things that are not yet cached.
  candidate_cache = [k for k in cache.keys() if cache[k] < 1]
  
  loc = get_attr(graph, "loc")
  mem = get_attr(graph, "mem")
  preds = get_preds(graph)
  
  chosen_key = None
  best_savings = 0
  for k in candidate_cache:
    savings = (runs[k] - 1.0) * tot(cache, k, runs, loc, preds)
    if savings >= best_savings and mem[k] <= memleft:
      chosen_key = k
      best_savings = savings
  
  cache[chosen_key] = 1.0
  return cache


def greedy_cache_select(graph, max_mem):
  cache = {k:0.0 for k in graph["vertices"].keys()}
  
  runs = get_r(cache, graph)  
  graph_mem = get_attr(graph, "mem")
  used_mem = cache_mem(graph_mem, cache)
  
  while used_mem < max_mem and still_room(cache, runs, graph_mem, max_mem-used_mem):
    print "Running!"
    cache = select_next(graph, cache, runs, max_mem-used_mem)
    runs = get_r(cache, graph)
    used_mem = cache_mem(graph_mem, cache)
    
  print "Done! "
  print "   Cache: %s" % str(cache)
  print "   Runs: %s" % str(runs)
  print "   Mem: %2.3f" % used_mem
  print "   Total: %2.3f" % tot(cache, str(graph["sink"]), runs, get_attr(graph, "loc"), get_preds(graph))
  
  return cache


def main():
  import sys
  graph = load_graph(sys.stdin)
  print graph
  print greedy_cache_select(graph, float(sys.argv[1])) 
  
if __name__ == "__main__":
  main()