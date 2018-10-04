#!/usr/bin/python3
from pyspark import SparkConf, SparkContext
import operator
import Levenshtein
import sys 

# Params
EXECUTORS=int(sys.argv[3])
PARTITIONS_1=100
PARTITIONS_2=10000

def main():

    # Create Spark Conf
    conf = (SparkConf()
             .setAppName("Parallel distance matrix computation")
    )   
    sc = SparkContext(conf = conf)

    # Parse Args
    points=[ s for s in  open(sys.argv[1], "r").read().splitlines() if len(s) > 1 ]
    distance_file=sys.argv[2]  
    distance = levenshtein_C

    # Create Distributed Elements
    # Get a vector like:
    #
    # ( 1 , [e1, e2, ... ])
    # ( 2 , [e1, e2, ... ])
    #      ....
    points_rdd = sc.parallelize( range(len(points)), PARTITIONS_1)
    
    # Compute cartesian product, and filter only the upper triangle matrix
    couples_rdd = points_rdd.cartesian(points_rdd).filter(lambda t: t[0] >= t[1] ).coalesce(PARTITIONS_2)


    # Compute distances for a batch of pairs
    def calc_distance (tups, points):

        for tup in tups:
            i1, i2 = tup
            p1 = points [i1]
            p2 = points [i2]
            d = distance(p1,p2)
            
            yield (i1, (i2, d))   
            # Emit distances for the lower triangle matrix
            if i1 != i2:
                yield (i2, (i1, d)) 
   
    couples_distance = couples_rdd.mapPartitions(lambda tups: calc_distance(tups, points) )
    
    
    # Create Distance Matrix
    # Get a vector like:
    #
    # ( 1 , [d1, d2, ... ])
    # ( 2 , [d1, d2, ... ])
    #      ....
    
    def get_distance_vector(t):
        i, dist_items = t
        dist_items_sorted = sorted (dist_items, key=operator.itemgetter(0))
        distances = [d for index, d in dist_items_sorted]
        return (i, distances)
    
    distributed_distance = couples_distance.groupByKey().map(get_distance_vector)
    
    # Prettify results
    def format_dist(t):
        i, dd = t
        dd_s = " ".join([str(d) for d in dd])
        s = str(i) + " " + dd_s
        return s
        
    distributed_distance_str = distributed_distance.map(format_dist)

    # Save on file
    distributed_distance_str.saveAsTextFile(distance_file)
    

# Normalized levenshtein distance
def levenshtein_C (s1,s2):
    return float(1.0-Levenshtein.ratio(s1,s2))


    
if __name__ == "__main__":
    main()



