
# Software Alchemy approach to k-NN regression 

# caknn() works on a distributed data frame/matix d as follows:
# 
# 1. For any data point x in a chunk, its neighbors are calculated only 
#    within that chunk.
# 
# 2. The value of the regression function at x is estimated as the mean
#    (or other statistic) of the Y values of the neighbors, excluding x.
# 
# 3. All the estimated regression function values are written to a
#    distributed file.
# 
# caknn.predict() does the following:
# 
# 1. Call fileread() to input the distributed file.
# 
# 2. Predict newcases from the estimated regression values, using 1-NN.


