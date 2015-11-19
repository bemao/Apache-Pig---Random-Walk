# Apache-Pig---Random-Walk
This script performs a random walk on a graph of specified form

The script expects a row-normalized transition matrix of the graph of the form:

node1,node2,weight

as well as a restart vector, X, which contains the node (or nodes) from which the graph is performed. 
For n nodes, this vector should be input in the form:

0,node 1,1/n <br>
0,node 2,1/n <br>
... <br>
0,node n,1/n <br>

