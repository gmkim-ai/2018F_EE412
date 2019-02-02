import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1])
# data is (src ID, dst ID) pair RDD
data = lines.map(lambda x : (int(x.split('\t')[0]), int(x.split('\t')[1]))).distinct()
# num_src2dst is (src ID, # of ways from src node) pair RDD
num_src2dst = data.groupByKey().map(lambda x : (x[0], len(x[1])))
# data4cal is (src ID, (dst ID, # of ways from src node)) RDD for calculate rank
data4cal = data.join(num_src2dst).cache()

# initial rank is (ID, 0.001) pair RDD 
rank = num_src2dst.map(lambda x : (x[0], 1.0/1000))
# In this code, don't make matrix M.
# Just each (src ID, ((dst ID, # of ways from src node), rank)) map to (dst Id, rank / # of ways from src node)
# and use reducebykey, So we can calculate new rank of each dst Id node
# Lastly, multiply 0.9 to sum of new rank, and add 0.0001 for taxation
for _ in range(50):
	rank = data4cal.join(rank) \
				   .map(lambda x : (x[1][0][0], x[1][1] / float(x[1][0][1]))) \
				   .reduceByKey(lambda a, b : a + b) \
				   .map(lambda x : (x[0], 0.9*x[1] + 0.0001))

# Before collect result, sort by descending order and print it
result = rank.sortBy(lambda x : x[1], ascending=False).collect()

for x in result[:10]:
   print('%d\t%f' % (x[0], x[1]))
sc.stop()

"""
RESULT
263	0.002158
537	0.002122
965	0.002056
243	0.001973
255	0.001937
285	0.001932
16	0.001908
126	0.001896
747	0.001896
736	0.001893
"""