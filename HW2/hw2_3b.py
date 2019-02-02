import sys
import numpy as np

with open(sys.argv[1], 'r') as f:
    lines = f.readlines()
    for i in range(len(lines)):
        lines[i] = lines[i].split(',')

num_lines = len(lines)

# In This Code, Utility Matrix M's shape is (#_unique_user, #_unique_movie)
# So 'user_id' and 'movie_id' have information that which index related to which ID
user_id = []
movie_id = []
for i in range(num_lines):
    user_id.append(int(lines[i][0]))
    movie_id.append(int(lines[i][1]))
movie_id = list(set(movie_id))   # find unique movie and user
user_id = list(set(user_id))
user = user_id.index(600)        # 'user' is matrix index of user ID '600'

num_movie = len(movie_id)
num_user = len(user_id)

# First, We fill zeros in M, and assign value from ratings.txt in right index
M = np.zeros((num_user, num_movie))   # Utility Matrix M
for i in range(num_lines):
    M[user_id.index(int(lines[i][0]))][movie_id.index(int(lines[i][1]))] = lines[i][2]
# Then, Calculate each user's average rating and 
# subtract user's average rating form each user's rating for only non-zero element
M_avg = np.sum(M, axis=1) / np.count_nonzero(M, axis=1)
for i in range(num_user):
    for j in range(num_movie):
        if M[i][j] != 0:
            M[i][j] = M[i][j] - M_avg[i]

# User-based method
# When we calculate cosine distance, compute inner product of "userID 600"'s and whole user's vector
# Then divide by normalized value of user vector.
# In This Code, do not divide by "userID 600"'s normalized value because of speed and
# no need for compare cosine distance. (divide by same value means nothing) 
cosine_simil = np.dot(M[user], M.T) / np.sqrt(np.sum(np.square(M), axis = 1))
simil_user = np.argsort(-cosine_simil)[1:11]  # find similar 10 user
simil_rating = np.vstack((M[simil_user[0]], M[simil_user[1]], M[simil_user[2]], M[simil_user[3]], M[simil_user[4]], M[simil_user[5]], M[simil_user[6]], M[simil_user[7]], M[simil_user[8]], M[simil_user[9]]))

# Then compute each movie's average rating(predict_rating) from 10 similar users
# Lastly, Find highest predict rating movie index from list using argsort
sum_rate = np.sum(simil_rating, axis=0)
cnt_zero = np.count_nonzero(simil_rating, axis=0)
for i in range(num_movie):
    if cnt_zero[i] != 0:
        sum_rate[i] = sum_rate[i] / cnt_zero[i]
top_movies = np.argsort(-sum_rate)

# From beginning, print movie ID and predict rating if movie ID is 1 to 1000
cnt = 0
for i in range(num_movie):
    if movie_id[top_movies[i]] <= 1000:
        print('%d\t%f' % (movie_id[top_movies[i]], sum_rate[top_movies[i]] + M_avg[user]))
        cnt += 1
        if cnt == 5:
            break

# Item-based method
# Same as above, we only divide by norm of column vector
# Then, if movie ID is 1 to 1000, normalize the vector using 'norm',
# And find similar 10 movie for each 1000 movies.
# Then average the rating of "User ID 600" to those similar movie so
# predict the 1 to 1000 movies rating.  
# 'movie_rating' has predicted rating of 1 to 1000 movies
inner_product = np.dot(M.T, M)
norm = np.sqrt(np.sum(np.square(M), axis = 0))
movie_rating = [0 for _ in range(1000)]

for i in range(num_movie):
    if movie_id[i] <= 1000:
        for j in range(num_movie):
            if norm[j] != 0:
                inner_product[i][j] /= norm[j]
        simil_movie = np.argsort(-inner_product[i])[1:11]
        #Now, average rating of similar movies
        cnt = 0
        val = 0
        for j in range(10):
            if M[user][simil_movie[j]] != 0:
                val += M[user][simil_movie[j]]
                cnt += 1
        if cnt != 0:
            movie_rating[movie_id[i] - 1] = val / float(cnt)

top_movies = np.argsort(-np.array(movie_rating))[:5]
for i in range(5):
    print('%d\t%f' % (top_movies[i] + 1, movie_rating[top_movies[i]] + M_avg[user]))