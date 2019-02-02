import sys
import itertools
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)

sc.setLogLevel("WARN")
lines = sc.textFile(sys.argv[1])
data = lines.map(lambda x : (str(x.split('\t')[0]), str(x.split('\t')[1]).split(',')))

result = data.flatMap(lambda x : itertools.combinations(x[1], 2)) \
			 .map(lambda x : ((min(x[0], x[1]), max(x[0], x[1])), 1)) \
			 .reduceByKey(lambda a, b : a + b) \
			 .map(lambda x : (x[0][0], (x[0][1], x[1]))) \
			 .join(data) \
			 .filter(lambda x : x[1][0][0] not in x[1][1]) \
			 .map(lambda x : (x[0], x[1][0][0], x[1][0][1])) \
			 .sortBy(lambda x : int(x[1]), ascending=True) \
			 .sortBy(lambda x : int(x[0]), ascending=True) \
			 .sortBy(lambda x : x[2], ascending=False) \
			 .collect()
for x in result[:10]:
   print('%s\t%s\t%d' % (x[0], x[1], x[2]))
sc.stop()

"""
RESULT
18739   18740   100
31506   31530   99
31492   31511   96
31511   31556   96
31519   31554   96
31519   31568   96
31533   31559   96
31555   31560   96
31492   31556   95
31503   31537   95

Algorithm
If 1's friends are 2, 3, 4, Then (2, 3), (2, 4), (3, 4) each have common friend 1. So I used itertools package for make combination in friends list.
1. make combination of friends List, So Each (a, b) have common friend. 
2. mapping this to ((a, b), 1) and reduce, so check How many a and b have common friends.
3. mapping this result to (a, (b, #ofcommonfriends)) and join original friends list data. Because We have to eliminate that a and b are friend case.
4. When Join, data form is (a, ((b, #ofcommonfriends), a's_friends_list))
5. So check that b is not in a's friends list using Filter
6. Then, make data form cleary like (a, b, #ofcommonfriends)
7. Finally, Sorting and collect. Take only top 10 data and print them.
"""