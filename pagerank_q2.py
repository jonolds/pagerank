from pyspark import SparkConf, SparkContext
import sys
import numpy as np

n = 1000
beta = 0.8
iterations = 40

#################################################################
def pagerank():
	def generateTuple(x):
		src, dst = x.split()
		return (int(src)-1, int(dst)-1)

	def generateM(x):
		src, dist_list = x
		deg = len(dist_list)
		return [(dist, src, 1./deg) for dist in dist_list]

	def generateR(x):
		r = np.zeros((n, 1))
		for i,v in x:
			r[i] = v
		return r

	conf = SparkConf()
	sc = SparkContext(conf=conf)
	data = sc.textFile(sys.argv[1]).map(lambda x: generateTuple(x)).distinct()	# (s, d)
	temp = data.groupByKey()	# (s, resultiterable d_list)
	M = temp.flatMap(lambda x: generateM(x))	# (d, s, v)

	r = np.ones((n, 1)) / n
	for _ in range(iterations):
		muliplications = M.map(lambda (i, j, v): (i, v*r[j]))
		r_rdd = muliplications.reduceByKey(lambda x, y : x + y)
		r_rdd = r_rdd.map(lambda (i, v): (i, beta*v+(1-beta)/n))
		r = generateR(r_rdd.collect())

	ascending = np.argsort(r.T) + 1
	descending = np.argsort(-r.T) + 1
	print 'top 5 in descending order: ', descending[0,0:5]	# [263 537 965 243 285]
	print 'bottom 5 in ascending order: ', ascending[0,0:5]	# [558  93  62 424 408]
	sc.stop()
#################################################################


#################################################################
def hits():
	def generateL(x):
		src, dst = x.split()
		return (int(src)-1, int(dst)-1, 1)

	def generateLT(x):
		src, dst = x.split()
		return (int(dst)-1, int(src)-1, 1)

	def generateVector(x):
		r = np.zeros((n, 1))
		for i,v in x:
			r[i] = v
		return r

	conf = SparkConf()
	sc = SparkContext(conf=conf)
	data = sc.textFile(sys.argv[1])
	L = data.map(lambda x: generateL(x)).distinct()
	LT = data.map(lambda x: generateLT(x)).distinct()

	a = np.ones((n,1))
	for _ in range(iterations):
		muliplications = L.map(lambda (i, j, v): (i, v*a[j]))
		h_rdd = muliplications.reduceByKey(lambda x, y : x + y)
		h = generateVector(h_rdd.collect())
		h = h / np.amax(h)

		muliplications = LT.map(lambda (i, j, v): (i, v*h[j]))
		a_rdd = muliplications.reduceByKey(lambda x, y : x + y)
		a = generateVector(a_rdd.collect())
		a = a / np.amax(a)

	h_ascending = np.argsort(h.T) + 1
	h_descending = np.argsort(-h.T) + 1
	print 'top 5 hubs in descending order: ', h_descending[0,0:5]	# [840 155 234 389 472]
	print 'bottom 5 hubs in ascending order: ', h_ascending[0,0:5]	# [ 23 835 141 539 889]
	a_ascending = np.argsort(a.T) + 1
	a_descending = np.argsort(-a.T) + 1
	print 'top 5 authorities in descending order: ', a_descending[0,0:5]	# [893  16 799 146 473]
	print 'bottom 5 authorities in ascending order: ', a_ascending[0,0:5]	# [ 19 135 462  24 910]
	sc.stop()
#################################################################

if __name__ == "__main__":
	pagerank()
	hits()
