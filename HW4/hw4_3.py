import sys

# read data
with open(sys.argv[1], 'r') as f:
    stream = f.readlines()
    stream = [int(s) for s in stream]

# change # of 1 to binary form
num_1 = sum(stream)
bin_num_1 = [int(x) for x in bin(num_1)[2:]]
# bin_num_1 = [1, 1, 1, 1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1]

# calcuate number of each bucket using binary form of # of 1
# In DGIM, we can do better estimation(predict) if we use many small size bucket.
# For example, [222](2 of bucket 1, 2 of bucket 2, 2 of bucket 4) is better than 
# [1110](1 of bucket 2, 1 of bucket 4, 1 of bucket 8)
# So, In this iteration, optimize list bin_num_1 to bucket_num for many small size bucket. 
bucket_num = bin_num_1
for index in range(len(bucket_num)):
    if index == 0:
        continue
    i = len(bucket_num) - index - 1
    if bucket_num[i] > 0 and bucket_num[i+1] == 0:
        bucket_num[i] -= 1
        bucket_num[i+1] = 2
        i += 1
        while i < len(bucket_num) - 1 and bucket_num[i] > 0 and bucket_num[i+1] == 0:
            bucket_num[i] -= 1
            bucket_num[i+1] = 2
            i += 1
index = 0
while bucket_num[index] == 0:
    index += 1
bucket_num = bucket_num[index:]
bucket_num.reverse()
# bucket_num = [1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 2, 1, 2, 2, 2]
# This is result of bucket_num. It mean, 1 bucket size of 1, ..., 2 buckets size of 256, 
# ..., 2 bucket size of 262144, 2 bucket size of 524288

# In dgim algorithm, each bucket save information of its endpoint, and its size.
# So, its endpoint save in dgim_endpoint list, and size save in dgim_size.
dgim_size = []
for i in range(len(bucket_num)):
    for _ in range(bucket_num[i]):
        dgim_size.append(pow(2, i))
# dgim_size = [1, 2, 4, 8, 16, 32, 64, 128, 256, 256, 512, 1024, 1024, 2048, 2048, 4096
# , 8192, 16384, 32768, 32768, 65536, 131072, 131072, 262144, 262144, 524288, 524288]

# Here, compute endpoint of each bucket using dgim_size going from last(latest) data 
# to first(oldest) data 
dgim_endpoint = []
bucket_size = dgim_size[::-1]
size = bucket_size.pop()
num = 0
for index in range(len(stream)):
    i = len(stream) - index - 1
    if stream[i] == 1 and num == 0:
        dgim_endpoint.append(i)
    if stream[i] == 1:
        num += 1
    if num == size:
        if len(bucket_size) != 0:
            size = bucket_size.pop()
        num = 0

# dgim_endpoint = [9999999, 9999993, 9999981, 9999965, 9999900, 9999810, 9999653, 
# 9999380, 9998745, 9997496, 9996231, 9993822, 9988622, 9983471, 9973404, 9963206, 9942639, 
# 9901543, 9819507, 9656672, 9492402, 9165096, 8511495, 7856611, 6547270, 5238510, 2621314]

# Now, using dgim_endpoint and dgim_size, estimate number of 1 of each k from argv.
# first, k < 0, then print 0
# Then find bucket that may involve this k query point in. call this bucket is x bucket
# If this x bucket's endpoint is same as k query point, Then print (front bucket size's sum + 1) 
# else, but if this x bucket's size is 1, Then we can estimate exact value because
# query point is between of next bucket's endpoint and bucket 1. print (front bucket size's sum + x bucket size(1))
# else, print (front bucket size's sum + (x bucket size / 2)). 
for i in range(len(sys.argv) - 2):
    k = int(sys.argv[i + 2])
    if k < 1:
        value = 0
    else:
        query = len(stream) - k
        index = 0
        while index < len(dgim_endpoint) and dgim_endpoint[index] >= query:
            index += 1
        index -= 1
        if dgim_endpoint[index] == query:
            value =  sum(dgim_size[:index]) + 1
        else:
            if dgim_size[index] == 1:
                value = sum(dgim_size[:index]) + dgim_size[index]
            else:
                value = sum(dgim_size[:index]) + int(dgim_size[index] / 2)
    print(value)