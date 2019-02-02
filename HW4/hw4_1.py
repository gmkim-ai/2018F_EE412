import sys
import numpy as np

# k_fold function make dataset from k-fold crossvalidation. 
# Here, k=10, and if index is 0, Then train_x is chunk 1 ... 9 and test_x is chunk 0.
# Same as test dataset.
def k_fold(k, index, feature, label):
    size = len(feature)
    chunk_size =  int(size / k)
    train_x = feature[:chunk_size * index] +  feature[chunk_size * (index + 1):size]
    train_y = label[:chunk_size * index] +  label[chunk_size * (index + 1):size]
    test_x = feature[chunk_size * index : chunk_size * (index + 1)]
    test_y = label[chunk_size * index : chunk_size * (index + 1)]
    return train_x, train_y, test_x, test_y

# read feature and label file
with open(sys.argv[1], 'r') as f:
    feature = f.readlines()
    for i in range(len(feature)):
        feature[i] = np.array([int(s) for s in feature[i].split(',')])

with open(sys.argv[2], 'r') as f:
    label = f.readlines()
    label = [int(l) for l in label]

# parameters
C = 0.1
learning_rate = 0.0005
epochs = 500

# First, iterate 10 time for cross-validation. Change dataset and initialize w, b again, and calcuate accuracy.
# Each iteration, interate epoch(500) times for update w and b using train dataset.
# learning rule is at textbook. sum_w is sum of -y*x that violate condition (y*(wx+b) >= 1)
avg_acc = 0
for index in range(10):
    train_x, train_y, test_x, test_y = k_fold(10, index, feature, label)

    w = np.zeros(len(feature[0]))
    b = 0

    for epoch in range(epochs):
        sum_w = 0
        sum_b = 0
        for i in range(len(train_x)):
            if train_y[i] * (np.dot(w, train_x[i]) + b) < 1:
                sum_w += -1 * train_y[i] * train_x[i]
                sum_b += -1 * train_y[i]
        w -= learning_rate * (w + C * sum_w)
        b -= learning_rate * (b + C * sum_b)

    num = 0
    for j in range(len(test_x)):
        if test_y[j] == 1 and np.dot(w, test_x[j]) + b >= 0:
            num += 1
        if test_y[j] == -1 and np.dot(w, test_x[j]) + b < 0:
            num += 1
    avg_acc += num

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