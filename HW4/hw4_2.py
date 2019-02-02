import sys
import numpy as np
from pyspark import SparkConf, SparkContext

# k_fold function return 2 RDD, that train dataset and test dataset
# Here, Each dataset is consists of (index, x, y).
# index is 0~9 value for check this data from which cross-validation iteration.
# If index=0, then this test data from chunk 1, and train data from chunk 2~10.
# Prepare data like this form because I parallelize k-fold cross validation too.
def k_fold(sc, k, feature, label):
    train_data = []
    test_data = []
    size = len(feature)
    chunk_size =  int(size / k)
    for index in range(k):
        train_x = feature[:chunk_size * index] +  feature[chunk_size * (index + 1):size]
        train_y = label[:chunk_size * index] +  label[chunk_size * (index + 1):size]
        train_data += list(zip([index for _ in range(len(train_x))], train_x, train_y))
        test_x = feature[chunk_size * index : chunk_size * (index + 1)]
        test_y = label[chunk_size * index : chunk_size * (index + 1)]
        test_data += list(zip([index for _ in range(len(test_x))], test_x, test_y))

    return sc.parallelize(train_data), sc.parallelize(test_data)

# cal_gradient function return sum of (index, (sum_of_-xy, sum_of_-y)).
# each sum_of_? value multiply by C and this use for update w and b.
# Here, sum_of_? value is calculated only viloate (y*(wx+b) >= 1)condition case.
# And use w and b list form, because each k-fold cross validation must do with different weight, bias
def cal_gradient(data, w, b):
    total = data.filter(lambda x : x[2] * (np.dot(w[x[0]], x[1]) + b[x[0]]) < 1) \
                .map(lambda x : (x[0], (-1 * x[2] * x[1], -1 * x[2]))) \
                .reduceByKey(lambda x, y : (x[0] + y[0], x[1] + y[1])) \
                .collect()
    return total

# cal_accuracy function return # of correct test case
# In here, index doesn't meaningful because we sum all correct test case
# and divide by len(testdata) finally.
def cal_accuracy(data, w, b):
    cnt = data.filter(lambda x : (x[2] == 1 and np.dot(w[x[0]], x[1]) + b[x[0]] >= 0) \
                                or x[2] == -1 and np.dot(w[x[0]], x[1]) + b[x[0]] < 0) \
              .count()
    return cnt

# read dataset
with open(sys.argv[1], 'r') as f:
    feature = f.readlines()
    for i in range(len(feature)):
        feature[i] = np.array([int(s) for s in feature[i].split(',')])

with open(sys.argv[2], 'r') as f:
    label = f.readlines()
    label = [int(l) for l in label]

conf = SparkConf()
sc = SparkContext(conf=conf)

# parameters
C = 0.1
learning_rate = 0.0005
epochs = 500

# prepare dataset and w, b with list form
# w[4] for index 4 (chunk 4 for test, chunk 1~3, 5~10 for train)
train_data, test_data = k_fold(sc, 10, feature, label)
w = [np.zeros(len(feature[0])) for _ in range(10)]
b = [0 for _ in range(10)]

# I parallelize k-fold cross validation and calculate sum of -yx or -x for update w, b
# I can't parallelize epoch(500) iteration because this each iteration should do with lastest update w, b.
# So I parallelize rest things. In here, each iteration, update w and b with cal_gradient function.
for epoch in range(epochs):
    res = cal_gradient(train_data, w, b)
    for i in range(len(res)):
        w[res[i][0]] -= learning_rate * (w[res[i][0]] + C * res[i][1][0])
        b[res[i][0]] -= learning_rate * (b[res[i][0]] + C * res[i][1][1])

# Also calculate accuracy with cal_accuracy function. 
# divide this value by len(testdata) become average accuracy
avg_acc = cal_accuracy(test_data, w, b)
sc.stop()

# print result
print(avg_acc / float(len(label)))
print(C)
print(learning_rate)

'''
RESULT
0.84
0.1
0.0005
'''