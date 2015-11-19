"""
This file runs a single RandomWalk algorithm (given in 'Repeated Random Walks on Genomic Scale Protein Networks.." Macropol et al.)
Expects a row-normalized transition matrix, P_T
A restart vector, X, which is has 0 for all entries except nodes of interest


Some of the embedding framework was done by consulting: https://techblug.wordpress.com/2011/07/29/pagerank-implementation-in-pig/
"""

from org.apache.pig.scripting import *
import sys

def main():

	pig_script = """
	
	S = LOAD '${in_data_orig}/*' USING PigStorage('\t') AS (row, column, value:float);
	X = LOAD '${in_data}/*' USING PigStorage('\t') AS (row, column, value:float);
	P_T = LOAD 'path/to/graph/*' USING PigStorage('\t') AS (column, row, value:float);
	
	--Multiply P_T by X
	A = JOIN P_T BY column FULL OUTER, X BY row;
  	B = FOREACH A GENERATE P_T::row AS m1r, X::column AS m2c, (P_T::value)*(X::value) AS value;
  	C = GROUP B BY (m1r, m2c);

  	multiplied = FOREACH C GENERATE group.$0 as row, group.$1 as column, SUM(B.value) AS value;
  	multiplied = FILTER multiplied BY $1 IS NOT NULL;

	right_half = FOREACH multiplied GENERATE row, column, (1-$alpha)*value AS value;
	
	left_half = FOREACH S GENERATE row, column, ${alpha}*value AS value;
	
	out_data = JOIN right_half BY (row, column) LEFT, left_half BY (row, column);
	out_data = FOREACH out_data GENERATE right_half::row AS row, right_half::column AS column, (left_half::value IS NULL?0:left_half::value) AS l_val, (right_half::value IS NULL?0:right_half::value) AS r_val;
  	out_data = FOREACH out_data GENERATE row, column, l_val+r_val AS value;
	
	diff_calc = JOIN out_data BY (row,column) FULL OUTER, X BY (row, column);
	diff_calc = FOREACH diff_calc GENERATE (out_data::value IS NULL?0:out_data::value) AS new_val,(X::value IS NULL?0:X::value) AS X_val;
	diff = FOREACH diff_calc GENERATE ABS(new_val-X_val) AS abs_diff;
	diff_gpd = GROUP diff ALL;
	max_diff = FOREACH diff_gpd GENERATE MAX(diff.abs_diff);
    
	STORE out_data INTO '${output}' USING PigStorage('\t');
	STORE max_diff INTO '${max_diff}' USING PigStorage('\t');

	"""
	
	alpha = 0.7    #alpha is the reset probability. higher alpha => the walkers stay closer to home
	in_data = "path/to/restart/vector"
	in_data_orig = "path/to/restart/vector"
	
	Pig.fs("rm -r " + "output/path")

  	P = Pig.compile(pig_script);	
	
	for i in range(10):
	    out = output/path/out/walk_"+str(i+1)
	    max_diff = base_path+"/output/new/temp/"+interest+"/out/max_diff_"+str(i+1)
	    output = out
	    Pig.fs("rm -r " + out)
	    Pig.fs('rm -r ' + max_diff)
	    stats = P.bind().runSingle()
	    if not stats.isSuccessful():
	      raise 'failed'
	    max_diff_value = float(str(stats.result("max_diff").iterator().next().get(0)))
	    print " max_diff_value = " + str(max_diff_value)
	      if max_diff_value < 0.00001:
	        print "done at iteration " + str(i+1)
	        break
	    in_data = out

if __name__ == '__main__':
    main()

