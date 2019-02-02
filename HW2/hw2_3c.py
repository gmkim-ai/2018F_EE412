#command : python hw2_3c.py path/to/ratings.txt path/to/movies.txt path/to/ratings_test.txt
import sys
import numpy as np

with open(sys.argv[1], 'r') as f:
	lines = f.readlines()
	for i in range(len(lines)):
		lines[i] = lines[i].split(',')

# Same as hw2_3b.py, Utility Matrix M's shape is (#_unique_user, #_unique_movie)
# So 'user_id' and 'movie_id' have information that which index related to which ID
user_id = []
num_lines = len(lines)
user_id = []
movie_id = []
for i in range(num_lines):
	user_id.append(int(lines[i][0]))
	movie_id.append(int(lines[i][1]))
movie_id = list(set(movie_id))
user_id = list(set(user_id))

num_movie = len(movie_id)
num_user = len(user_id)

# Make utility matrix M, then calculate each user's average rating and 
# subtract user's average rating form each user's rating for only non-zero element
# Here, if rating - average_rating == 0, Then change it to 0.00001
# Because this rating must involve in gradient descent process when UV decomposition
M = np.zeros((num_user, num_movie))
for i in range(num_lines):
	M[user_id.index(int(lines[i][0]))][movie_id.index(int(lines[i][1]))] = lines[i][2]
M_avg = np.sum(M, axis=1) / np.count_nonzero(M, axis=1)
for i in range(num_user):
	for j in range(num_movie):
		if M[i][j] != 0:
			M[i][j] = M[i][j] - M_avg[i]
			if M[i][j] == 0:
				M[i][j] == 0.00001

# Matrix-Factorization Using Gradient Descent
# Here, Cost = (M_ij - [UV]_ij)^2 + (beta / 2) * (L2_norm(U)^2 + L2_norm(V)^2) and
# change each element to element = element - learning_rate * (d_Cost / d_element)
K = 100
epoch = 100
learning_rate = 0.01
regularize_beta = 0.01
U = (np.random.rand(num_user, K) - 0.5) / 10
V = (np.random.rand(K, num_movie) - 0.5) / 10

for step in range(epoch):
	#Train
	for i in range(num_user):
		for j in range(num_movie):
			if M[i][j] != 0:
				cost_ij = M[i][j] - np.dot(U[i, :], V[:, j])
				for k in range(K):
					U[i][k] += learning_rate * (2 * cost_ij * V[k][j] - regularize_beta * U[i][k])
					V[k][j] += learning_rate * (2 * cost_ij * U[i][k] - regularize_beta * V[k][j])

	#Validation
	if (step + 1) % 5 == 0:	
		M_ = np.dot(U, V)
		cost = 0
		for i in range(num_user):
			for j in range(num_movie):
				if M[i][j] != 0:
					cost += (M[i][j] - M_[i][j]) ** 2 + (regularize_beta / 2.0) * (np.sum(np.square(U[i, :])) + np.sum(np.square(V[:, j])))
		if cost < 0.01 * num_lines:
			break
M_ = np.dot(U, V)

# I use 'movies.txt' for rating unseen movie in 'ratings.txt'
# Here, I save the genre of each movie in list
with open(sys.argv[2], 'r') as f:
	movie_lines = f.readlines()
	for i in range(len(movie_lines)):
		movie_lines[i] = movie_lines[i].split(',')

movies_id = []
movies_type = []
for i in range(len(movie_lines)):
	movies_id.append(int(movie_lines[i][0]))
	movies_type.append(movie_lines[i][-1][:-1].split('|'))

# Open 'ratings_test.txt' and put predict ratings to 'output.txt'
with open(sys.argv[3], 'r') as f:
	test_lines = f.readlines()
	for i in range(len(test_lines)):
		test_lines[i] = test_lines[i].split(',')

with open('output.txt', 'w') as f:
	for i in range(len(test_lines)):
		# movie in 'ratings.txt'
		# if this movie not in 'ratings.txt', Then index function make error and go to except part
		try:
			movie = movie_id.index(int(test_lines[i][1]))
			user = user_id.index(int(test_lines[i][0]))
			rate = M_[user][movie] + M_avg[user]
			if rate > 5:
				rate = 5         # cliped predict rating
			if rate < 0.5:
				rate = 0.5
			f.write('%s,%s,%f,%s' % (test_lines[i][0], test_lines[i][1], rate, test_lines[i][3]))
		# movie not in ratings.txt recommend using movies.txt
		# Find user's all rated movie which has common genre with this movie
		# And average those movies ratings.
		except:
			movies = movies_id.index(int(test_lines[i][1]))
			user = user_id.index(int(test_lines[i][0]))
			cnt = 0
			val = 0
			for j in range(num_movie):
				if M[user][j] != 0 and set(movies_type[movies]) & set(movies_type[movies_id.index(movie_id[j])]) != set():
					val += M[user][j]
					cnt += 1
			if cnt != 0:
				rate = (val / float(cnt)) + M_avg[user]
				if rate > 5:
					rate = 5       # cliped predict rating
				if rate < 0.5:
					rate = 0.5
			else:
				rate = M_avg[user]
			f.write('%s,%s,%f,%s' % (test_lines[i][0],test_lines[i][1], rate, test_lines[i][3]))