import numpy
#import matplotlib.pyplot as plt
from numpy import array
from cvxopt import matrix, solvers

dot = numpy.dot

def frank_wolfe_constrained_qp(n, A, b, c, B, max_iter=1000):
  """Solves a constrained quadratic program with Frank-Wolfe."""
  
  #This function is our QP.
  def f(w):
    return dot(dot(w.T,A),w) + dot(b,w)

  #This function is the gradients of the QP.
  def grad_f(w):
    return 2.*dot(A,w) + b

  #G and h set up the constraints of the QP. we have 2n+1 of them.
  G = numpy.zeros((2*n+1,n))
  h = numpy.zeros((2*n+1,1))
  for i in range(n):
    G[2*i,i] = -1. #The even ones are w_i >= 0
    G[2*i+1,i] = 1. #The odd ones are w_i <= 1
    h[2*i] = 0.
    h[2*i+1] = 1.

    G[2*n,i] = c[i] #The last one is that \sum_{c_i*w_i} <= B
    
  h[2*n] = B #RHS of the last constraint.
  Gn = G.T.tolist()
  hn = h.T.tolist()

  w = numpy.zeros(n) #Initialize w to something feasible.

  print 'f(w)='+str(f(w)) #Print the starting value.
  
  solvers.options['show_progress'] = False
  G = matrix(Gn)
  h = matrix(h)
  
  for t in range(max_iter):
    grad = grad_f(w)

    c = matrix(grad)

    sol = solvers.lp(c, G, h)

    y = array(sol['x']).T[0]

    gamma = 2./(2.+t)
    w = (1-gamma)*w + gamma*y

    if t % 100 == 0:
      print 'f(w)='+str(f(w))
  
  return w
