"""
script for power iteration example
"""

import numpy as np

#covariance matrix
M = np.matrix([[(14.0/3.0), 6],[6, 9]])
#init (random) unit vector
x_init = np.array([1,1])

for i in range(1,10):
    x_i = (np.linalg.matrix_power(M, i)).dot(x_init)
    x_prev = (np.linalg.matrix_power(M, i-1)).dot(x_init)

    print("iteration: ",i)
    print("x_i: ",x_i)
    print("x_i-1: ",x_prev)

    x_i_ratio = x_i/np.linalg.norm(x_i)
    x_prev_ratio = x_prev/np.linalg.norm(x_prev)
    
    print("x_i_norm: ",x_i_ratio)
    print("x_i-1_norm: ",x_prev_ratio)
    print("delta: ",x_i_ratio-x_prev_ratio)
    print("----------------")
    
    
    if(np.allclose(x_i_ratio, x_prev_ratio)):
        print("convergence reached: ",x_i_ratio)
        break
    
    x_prev = x_i