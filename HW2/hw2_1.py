import sys
from pyspark import SparkConf, SparkContext

# calculate (distance)^2 between two list (each list is vector)
# In This Code, Use (distace)^2 instead of (distance) for speed
def dist_square(a, b):
    val = 0
    for i, j in zip(a, b):
        val += (float(i) - float(j)) ** 2
    return val

# return cluster number which this 'point' will be assigned
# calculate (distance)^2 between point and centroids, and find min value 
def assign_cluster(point, init_points_pos):
	cluster = -1
	min_val = -1
	for i in range(len(init_points_pos)):
		dist =  dist_square(init_points_pos[i], point)
		if min_val == -1 or dist < min_val:
			min_val = dist
			cluster = i + 1
	return cluster

# When 'points' are positions of points in one cluster, return diameter
# compute sqrt(max_val) because we compare with (distance)^2
def cal_diameter(points):
	max_val = 0
	for i in range(len(points) - 1):
		for j in range(i + 1, len(points)):
			dist = dist_square(points[i], points[j])
			if dist > max_val:
				max_val = dist
	return max_val ** (1/2.0)

# Initialization (Only Python) 
num_clusters = int(sys.argv[2])
with open(sys.argv[1], 'r') as f:
    lines = f.readlines()
    for i in range(len(lines)):
        lines[i] = lines[i].split()

num_points = len(lines)

init_points = []  # list of centroids
min_dist_from_init_points = [0 for _ in range(num_points)] # list of minumum distance from centroids

new_point = 0
init_points.append(new_point) # add first point to centroids list

# if number of clusters is more than 1, then compute next centroids which
# most far point from centroid(first point).
# save whole points distance from centroid in min_dist_from_init_points 
if num_clusters > 1:
	for i in range(num_points):
	    min_dist_from_init_points[i] = dist_square(lines[i], lines[new_point])
	new_point = sorted(range(num_points), reverse = True, key=lambda k: min_dist_from_init_points[k])[0]
	init_points.append(new_point)    

# if number of cluster is more than 2, then we must find most far point
# which distance is minimum distance with centroids.
# So we update min_dist_from_init_points list if new centroids is
# more close than original centroids. 
for k in range(num_clusters - 2):
    for i in range(num_points):
        min_dist_from_init_points[i] = min(dist_square(lines[i], lines[new_point]), min_dist_from_init_points[i])
    new_point = sorted(range(num_points), reverse = True, key=lambda k: min_dist_from_init_points[k])[0]
    init_points.append(new_point)

# List 'init_points' is list of k centroids
# List 'init_points_pos' is list of position of k centroids
init_points_pos = []
for i in range(num_clusters):
	init_points_pos.append(lines[init_points[i]])

# Implement k-Means using Spark
conf = SparkConf()
sc = SparkContext(conf=conf)

# First map list(vector) x to (cluster_number, x) using assign_cluster function
# Then group RDD with Key that cluster number, so RDD form [cluster_number, list(points)]
# Lastly, map (cluster_number, list(points)) to (cluster_number, diameter) using cal_diameter function
lines_RDD = sc.textFile(sys.argv[1])
result = lines_RDD.map(lambda x : x.split(' ')) \
				  .map(lambda x : (assign_cluster(x, init_points_pos), x)) \
				  .groupByKey() \
				  .map(lambda x : (x[0], cal_diameter(list(x[1])))) \
				  .collect()

# Compute average diameter from result
sc.stop()
diameter_sum = 0
for i in range(num_clusters):
	diameter_sum += result[i][1]
print('%f' % (diameter_sum / float(num_clusters)))