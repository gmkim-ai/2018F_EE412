import re
import math
import random
import time

#start = time.time()
with open("articles.txt", 'r') as f:
    lines = f.readlines()
    doc_id = []
    doc = []
    whole_shingle = []
    for i in range(len(lines)):
        space = lines[i].index(' ')
        doc_id.append(lines[i][:space])
        split_doc = re.split(r'\s+', re.sub(r'[^a-z ]+', '', lines[i][space + 1:].lower()))
        doc.append(list(set([tuple(split_doc[j:j + 3]) for j in range(len(split_doc) - 2)])))
        whole_shingle += doc[i]
    shingles = list(set(whole_shingle))
    row_num = len(shingles)
    doc_num = len(doc)
    dic = dict(zip(shingles, range(row_num)))

prime = 0
num = row_num + 1
while prime == 0:
    for i in range(2, int(math.sqrt(num)) + 1):
        if num % i == 0:
            break
        if i == int(math.sqrt(num)):
            prime = num
    num += 1

hash_a = random.sample(range(prime), 120)
hash_b = random.sample(range(prime), 120)

hash_list = [[0 for _ in range(row_num)] for _ in range(120)] 
for i in range(120):
    a = hash_a[i]
    b = hash_b[i]
    for j in range(row_num):
        hash_list[i][j] = (a * j + b) % prime
hash_list = zip(*hash_list)

signature = [[999999 for _ in range(120)] for _ in range(doc_num)] 

for i in range(doc_num):
    document = doc[i]
    for j in range(len(document)):
        hash_value = hash_list[dic[document[j]]]
        for k in range(120):
            if signature[i][k] > hash_value[k]:
                signature[i][k] = hash_value[k]

result = []
for i in range(doc_num - 1):
    for j in range(i + 1, doc_num):
        cnt = 0
        for k in range(120):
            if signature[i][k] == signature[j][k]:
                cnt += 1
        if (cnt / 120.0) >= 0.9:
            result.append((i, j, cnt / 120.0))

candidate = sorted(result, key=lambda x: x[2], reverse=True)
for i in range(len(candidate)):
    print('%s\t%s\t%.4f' % (doc_id[candidate[i][0]], doc_id[candidate[i][1]], candidate[i][2]))

#end = time.time()
#print("Time : %.2f seconds" % (end - start))

"""
RESULT
t8413   t269    1.0000
t1621   t7958   1.0000
t448    t8535   1.0000
t3268   t7998   0.9833
t980    t2023   0.9667

Shingle Rule
I use rule 'case 2' in classum that consider the shingle unit as a word.
1. change all document alphabet lower.
2. Use given parse code re.split(r'\s+', re.sub(r'[^a-z ]+', '', content))
3. Then make 3-shingle that each shingle is a word that splited by parsing code
Ex) "Troops loyal to Afghan President Burhanuddin Rabbani ~"
-> ['troops', 'loyal', 'to', 'afghan', 'president', 'burhanuddin', 'rabbani', ~]
-> [('troops', 'loyal', 'to'), ('loyal', 'to', 'afghan'), ('to', 'afghan', 'president'), ~]

Time
83.83 seconds

"""