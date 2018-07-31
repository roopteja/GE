# Spark program to find the sub vector closest to the query vector
import os
import sys
os.environ['SPARK_HOME']="spark_home"
os.environ['PYSPARK_PYTHON']=sys.executable
try:
    from pyspark import SparkContext, SparkConf
    print("Successfully imported Spark modules")
except ImportError as e:
    print("Error Importing Spark Modules", e)
    sys.exit(1)

# set up the configuration with the app name as an identifier (optional)
conf = SparkConf().setAppName("SparkProb")
sc = SparkContext(conf=conf)

# read the query file and add the contents to a tuple
query_file = open('query_vector.txt', 'r')
for line in query_file.readlines():
    strlist = line.split(',')
    Q = tuple(map(float, strlist))

# transform the rdd - converts the string into a data vector for processing
def loadVectors(data_file):
    data_vector = []
    strlist = data_file[1].split(',')
    for str in strlist:
        data_vector.append(float(str))
    data = [data_file[0], data_vector]
    return data

# get the closest sub vector for each data vector using euclidian distance
def processVector(data):
    data_vector = data[1]
    vect_len = len(data_vector)
    query_len = len(query_vector.value)
    result = ''
    if vect_len >= query_len:
        min_dist = -1
        sub_vect_idx = -1
        end_index = vect_len - query_len + 1
        for i in range(end_index):
            sum = 0
            for j in range(query_len):
                sum += (data_vector[i+j] - query_vector.value[j]) ** 2
            dist = sum ** (0.5)
            if min_dist == -1 or min_dist > dist:
                min_dist = dist
                sub_vect_idx = i
        result = 'Data Vector: D[' + data[0] + ']  Min Distance: ' + format(min_dist, '.3f') + ' Start index: ' + str(sub_vect_idx)
    return str(result)

# read the data vector files from the directory into rdd for processing
filerdd = sc.wholeTextFiles("/home/ubuntu/DataVectors/*.txt")
# broadcast the query vector so that it can be retrieved at the executors
query_vector = sc.broadcast(Q)
# transform the rdd
vector_result_rdd = filerdd.map(loadVectors).map(processVector)
# collect the result for each data vector
vector_result_rdd.collect()