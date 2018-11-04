import sys
from pyspark import SparkConf, SparkContext

MAX_ITER = 40

def compute_contribs(neighbors, rank):
  share = rank / float(len(neighbors))
  for n in neighbors:
    yield (n, share)

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# Load the data
data = sc.textFile(sys.argv[1]).map(lambda line: line.split('\t')).map(lambda line: (line[0], line[1]))
links = data.distinct().groupByKey().cache()
N = float(links.count())
ranks = links.keys().map(lambda page: (page, 1.0 / N))

for _ in range(MAX_ITER):
  contribs = links.join(ranks).flatMap(lambda (page, (neighbors, rank)): compute_contribs(neighbors, rank))
  ranks = contribs.reduceByKey(lambda v1, v2: v1 + v2).map(lambda (page, rank): (page, rank * 0.80 + 0.20 / N))

ranks = ranks.map(lambda (page, rank): (rank, page)).cache()

for top in ranks.sortByKey(False).take(5):
  print "TOP: %s = %f" % (top[1], top[0])

for bottom in ranks.sortByKey().take(5):
  print "BOTTOM: %s = %f" % (bottom[1], bottom[0])

sc.stop()
