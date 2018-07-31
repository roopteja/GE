# function to return the euclidian distance between two vectors
def euclidianDist(query, vector):
    sum = 0
    query_len = len(query)
    for i in range(query_len):
        sum += (vector[i] - query[i])**2
    return sum ** (0.5)

# function to return the euclidian distance between two vectors
def process(Q, D):
    query_len = len(Q)
    vector_index = 0
    # process each data vector
    for vector in D:
        vect_len = len(vector)
        # validate the size of the vectors
        if vect_len >= query_len:
            min_dist = -1
            sub_vect_idx = -1
            end_index = vect_len - query_len + 1
            # get the distance for each sub vector
            for i in range(end_index):
                dist = euclidianDist(Q, vector[i:(i + query_len)])
                if min_dist == -1 or min_dist > dist:
                    min_dist = dist
                    sub_vect_idx = i
            print('Data Vector: D[{vector}]  Min Distance: {dist} Start index: {index}'.format(vector=vector_index,
                                                                                               dist=format(min_dist,
                                                                                                           '.3f'),
                                                                                               index=sub_vect_idx))
        vector_index += 1

if __name__ == "__main__":
    #Q = ()
    #D = []
    #query_file = open('query_vector.txt', 'r')
    #for line in query_file.readlines():
    #    strlist = line.split(',')
    #    Q = tuple(map(float, strlist))
    #data_vector_file = open('data_vector1.txt', 'r')
    #for line in data_vector_file.readlines():
    #    strlist = line.split(',')
    #    D.append(tuple(map(float, strlist)))
    Q = (0.0, 1.0, 1.0)
    D = [(0.00, 1.16, 0.69, 0.83, 2.11, 0.00, 1.00, 1.00, 4.12, 2.64, 2.80),(5.29, 5.16, 4.43, 4.09, 3.72, 5.41, 5.38, 5.35, 4.67, 5.92, 4.18)]
    process(Q,D)



