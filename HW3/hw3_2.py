import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1])
# src2dst is (src ID, dst ID) pair RDD
src2dst = lines.map(lambda x : (int(x.split('\t')[0]), int(x.split('\t')[1]))).distinct().cache()
# dst2src is (dst ID, src ID) pair RDD
dst2src = lines.map(lambda x : (int(x.split('\t')[1]), int(x.split('\t')[0]))).distinct().cache()

# initial h is (ID, 1) pair RDD 
h = sc.parallelize([(i+1, 1) for i in range(1000)])
# In this code, don't make matrix L and L^T.
# first, each (src ID, (dst ID, src ID's h)) map to (dst Id, src ID's h)
# and use reducebykey, So we can calculate new a of each dst Id node.
# Then, we should calculate max value of a, and map again for normalized a.
# Second, each (dst Id, (src ID, dst ID's a)) map to (src Id, dst ID's a)
# and same as above, calculate new h of each src ID node and normalized it.
# Iterate this 50 times.
for _ in range(50):
    unnormalized_a = src2dst.join(h) \
               .map(lambda x : (x[1][0], x[1][1])) \
               .reduceByKey(lambda a, b : a + b)
    max_a = float(unnormalized_a.max(lambda x : x[1])[1])
    a = unnormalized_a.map(lambda x : (x[0], x[1] / max_a))

    unnormalized_h = dst2src.join(a) \
               .map(lambda x : (x[1][0], x[1][1])) \
               .reduceByKey(lambda a, b : a + b)
    max_h = float(unnormalized_h.max(lambda x : x[1])[1])
    h = unnormalized_h.map(lambda x : (x[0], x[1] / max_h))

# Before collect result, sort each rdd by descending order and print it
res_h = h.sortBy(lambda x : x[1], ascending=False).collect()
res_a = a.sortBy(lambda x : x[1], ascending=False).collect()

for x in res_h[:10]:
   print('%d\t%f' % (x[0], x[1]))
for x in res_a[:10]:
   print('%d\t%f' % (x[0], x[1]))
sc.stop()

"""
RESULT
840 1.000000
155 0.949962
234 0.898665
389 0.863417
472 0.863284
444 0.822972
666 0.800714
499 0.796615
737 0.774688
137 0.771515
893 1.000000
16  0.963557
799 0.951016
146 0.924670
473 0.899866
624 0.892220
533 0.883241
780 0.880036
494 0.874988
130 0.846547
"""