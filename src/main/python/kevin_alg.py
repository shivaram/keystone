import numpy
#import matplotlib.pyplot as plt
from numpy import array

dot = numpy.dot

n = 10
A = numpy.random.rand(n,n)
b = -numpy.ones(n)*3 + numpy.random.randn(n)
c = numpy.ones(n)
B = 0.8*n
def f(w):
	return dot(dot(w.T,A),w) + dot(b,w)

def grad_f(w):
	return 2.*dot(A,w) + b


G = numpy.zeros((2*n+1,n))
h = numpy.zeros((2*n+1,1))
for i in range(n):
	G[2*i,i] = -1.
	G[2*i+1,i] = 1.
	h[2*i] = 0.
	h[2*i+1] = 1.

	G[2*n,i] = c[i]
h[2*n] = B
Gn = G.T.tolist()
hn = h.T.tolist()

w = numpy.zeros(n)

print 'f(w)='+str(f(w))

max_iter = 1000
for t in range(max_iter):

	grad = grad_f(w)

	from cvxopt import matrix, solvers
	c = matrix(grad)
	G = matrix(Gn)
	h = matrix(h)
	solvers.options['show_progress'] = False
	sol = solvers.lp(c, G, h)

	y = array(sol['x']).T[0]

	gamma = 2./(2.+t)
	w = (1-gamma)*w + gamma*y

	# print 
	# print 'grad='+str(grad)
	# print 'y='+str(y)
	# print 'w='+str(w)
	# print 'sum(w)='+str(sum(w))
	if t % 10 == 0:
		print 'f(w)='+str(f(w))
