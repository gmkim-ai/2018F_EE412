import sys
import itertools

with open(sys.argv[1], 'r') as f:
    lines = f.readlines()
    whole_items = []
    for i in range(len(lines)):
        lines[i] = lines[i].split()
        whole_items += lines[i]
    items = list(set(whole_items))
    for i in range(len(lines)):
        for j in range(len(lines[i])):
            lines[i][j] = items.index(lines[i][j])

item_count = [0 for _ in range(len(items))]

for i in range(len(lines)):
    for j in range(len(lines[i])):
        item_count[lines[i][j]] += 1

freq_items = [0 for _ in range(len(items))]

freq_item_num = 0
for i in range(len(freq_items)):
    if item_count[i] >= 200:
        freq_item_num += 1
        freq_items[i] = freq_item_num
    else:
        freq_items[i] = 0
print(freq_item_num)

freq_lines = [[] for _ in range(len(lines))]
for i in range(len(lines)):
    for j in range(len(lines[i])):
        if freq_items[lines[i][j]] != 0:
            freq_lines[i].append(lines[i][j])

pair_count = [0 for _ in range(int(freq_item_num * (freq_item_num - 1) / 2.0))]

for i in range(len(freq_lines)):
    pairs = [(i, j) for i, j in itertools.combinations(freq_lines[i], 2)]
    for j in range(len(pairs)):
        a = freq_items[pairs[j][0]]
        b = freq_items[pairs[j][1]]
        if a > b:
            temp = a
            a = b
            b = temp
        pair_count[int((a - 1)*(freq_item_num - (a / 2.0)) + b - a - 1)] += 1

freq_pair_num = 0
for i in range(len(pair_count)):
    if pair_count[i] >= 200:
        freq_pair_num += 1
print(freq_pair_num)

top_pair = sorted(zip(range(len(pair_count)), pair_count), key=lambda x: x[1], reverse=True)[:10]

for i in range(len(top_pair)):
    k = top_pair[i][0] + 1
    for x in range(1, freq_item_num + 1):
        if (x - 1)*(freq_item_num - (x / 2.0)) + 1 > k:
            break
    a = x - 1
    b = int(k + a - (a - 1)*(freq_item_num - (a / 2.0)))
    print('%s\t%s\t%d' % (items[freq_items.index(a)], items[freq_items.index(b)], top_pair[i][1]))


"""
RESULT
363
328
DAI62779	ELE17451	1592
FRO40251	SNA80324	1412
DAI75645	FRO40251	1254
GRO85051	FRO40251	1213
GRO73461	DAI62779	1139
DAI75645	SNA80324	1130
FRO40251	DAI62779	1070
SNA80324	DAI62779	923
DAI62779	DAI85309	918
ELE32164	GRO59710	911

"""
