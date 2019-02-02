import sys
import re
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1])

counts = lines.flatMap(lambda x : re.split(r'[^\w]+', x)) \
              .map(lambda x : x.lower()) \
              .distinct() \
              .map(lambda x : str(x)[:1]) \
              .filter(lambda x : x.isalpha()) \
              .map(lambda x : (x, 1)) \
              .reduceByKey(lambda a, b : a + b)

counts.saveAsTextFile(sys.argv[2])
sc.stop()