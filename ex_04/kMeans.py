'''
Created on Nov 20th, 2015
@author: Sabrina Friedl
@co-author: Christian Frey
'''

import numpy as np
import matplotlib.pyplot as plt
import scipy.spatial as sci
from pyspark import SparkConf, SparkContext
import re


#************************************* Initialization of Spark Context ***********************
# Configure the Spark Environment
sparkConf = SparkConf().setAppName("kMeans").setMaster("local")
sc = SparkContext (conf = sparkConf)


# Read input data
#birchData = sc.textFile("C:/Users/Sabrina/PycharmProjects/Tutorial4/s2.txt")  #Change to your path
#p_points = birchData.map(lambda line: re.sub(r"\s+", " ", line.strip())).map(lambda d: np.array(d.split(), dtype=float)).cache()

# Or create your own points
points = [(1,4.5), (1,3.5), (0.5, 3), (1.5,3), (3,0.5), (2,1), (2.5,1.5), (3.5,1), (3, 2), (4,4), (4,5), (3, 5), (3.5, 4),(5, 3.5), (5,5)]
vectors = [np.array(point) for point in points]
p_points = sc.parallelize(vectors) #rdd object for point list
p_points.cache()

##---------------------------------------------------------------------------------------


#********************************* Initialization & Configuration ****************************************
k = 3 #number of clusters
no_iter = 5 #number of iterations

#initialize k centroids
centroids = p_points.takeSample(False, k)
print(centroids)

#list of intermediate centroids, only saved for plotting
intermed_centroids = []
intermed_centroids.append(centroids)

#-------------------------------------------------------------------------------------------------------------


#********************************* Functions for Map and Reduce  ****+++**************************************
#calculates distances of point to all centroids and assigns point to closest centroid
def assign_to_centroid(point):
    bestCentroid = None
    minDist = float('inf')
    for i in range(len(centroids)): #loop over centroids
        c = centroids[i]
        dist = sci.distance.euclidean(point, c) #calculate L2 distance
        if (bestCentroid is None) | (dist < minDist):
            minDist = dist
            bestCentroid = i #ID of closest centroid = clusterID
    return (bestCentroid, point)


#calculates mean of points in a cluster and returns it as new centroid
def calculate_new_centroid(*cluster_points):
    sum_vector = [0, 0]
    n = 0 #number of points in cluster
    for point in cluster_points:
        sum_vector += point
        n += 1
    return sum_vector/n #output of reduce function = new cluster center


# Just for Plotting ----------------------------------------------------------------------------
class ClusterData:
    def __init__(self, id):
        self.clusterID = id;
        self.x = []
        self.y = []

    def appendToX (self, value):
        self.x.append(value)

    def appendToY (self, value):
        self.y.append(value)

    def setClusterId (self, value):
        self.clusterID = value

# save intermediate results for plotting
result_i = dict() #result of iteration i
def saveResultForIteration (iter_ID, assignments):
    if iter_ID not in result_i.keys():
        result_i[iter_ID] = {}
    for i in range (0, len(assignments)):
        clusterID = assignments[i][0]
        if clusterID not in result_i[iter_ID].keys():
            result_i[iter_ID][clusterID] = ClusterData(clusterID)
        result_i[iter_ID][clusterID].appendToX(assignments[i][1][0])
        result_i[iter_ID][clusterID].appendToY(assignments[i][1][1])


##*************************************** K-Means with MapReduce *******************************++++
j = 0
delta = float('inf')

while j < no_iter: #actually: while delta > 0
    print('ITERATION ', j+1)
    #####MAP: assign points to centroids
    assignments = p_points.map(assign_to_centroid).cache() #point -> (clusterID, point)
    assigned = assignments.collect()
    saveResultForIteration(j, assigned) #for plotting
    print("Assignments: ", assigned)

    ######REDUCE: calculate new centroids
    c_new = assignments.reduceByKey(calculate_new_centroid) # (clusterID, [p1, p2,..]) -> (clusterID, new centroid)
    centroids_new = [c[1] for c in c_new.collect()] #get new centroids
    print('New Centroids: ', centroids_new)

    #Calculate movement of centroid coordinates
    delta = 0
    for i in range(k):
        delta += sum(abs(centroids_new[i] - centroids[i]))
    print('Delta: ', delta)

    centroids = centroids_new #use new centroids for next iteration
    intermed_centroids.append(centroids) #just for plotting

    j += 1





#--------------------------------------------------------------------------------------
# Plotting
curr_pos = 0 #iteration
fig = plt.figure()
ax1 = fig.add_subplot(111)
colors = ['b', 'g', 'r', 'y', 'm', 'c']

def key_event(e):
    global curr_pos
    if e.key == "right":
        curr_pos = curr_pos + 1
    elif e.key == "left":
        curr_pos = curr_pos - 1
    else:
        return
    curr_pos = curr_pos % len(result_i)
    ax1.cla()

    print("Iteration: ", curr_pos)
    #Points
    for clusterID in result_i[curr_pos].keys():
        ax1.scatter(result_i[curr_pos][clusterID].x, result_i[curr_pos][clusterID].y, s=30, c=colors[clusterID], alpha=0.4)

    #Centroids
    xArr = []
    yArr = []
    centroid_colors = []
    for i in range (0, len(intermed_centroids[curr_pos])):
        xArr.append(intermed_centroids[curr_pos][i][0])
        yArr.append(intermed_centroids[curr_pos][i][1])
        centroid_colors.append(colors[i])

    ax1.scatter(xArr, yArr, s=40, c=centroid_colors)
    fig.canvas.draw()

fig.canvas.mpl_connect('key_press_event', key_event)

#Points
for clusterID in result_i[curr_pos].keys():
    ax1.scatter(result_i[curr_pos][clusterID].x, result_i[curr_pos][clusterID].y, s=30, c=colors[clusterID], alpha=0.4)

#Centroids
xArr = []
yArr = []
centroid_colors = []
for i in range (0, len(intermed_centroids[curr_pos])):
    xArr.append(intermed_centroids[curr_pos][i][0])
    yArr.append(intermed_centroids[curr_pos][i][1])
    centroid_colors.append(colors[i])

ax1.scatter(xArr, yArr, s=40, c=centroid_colors)
fig.canvas.draw()

plt.show()
