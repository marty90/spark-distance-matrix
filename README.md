# Spark Distance Matrix Calculator

This piece of code computes a distance matrix for a set of input strings. It uses Apache Spark to achieve parallelism.

For any information or request write to [martino.trevisan@polito.it](mailto:martino.trevisan@polito.it).

## Prerequisites
You need (a cluster with) Apache Spark, and Python.
The only needed library is python-Levenshtein. You can easily install it with:
```
pip install python-Levenshtein
```

## Input and output format
The script expects as input a single file containing the target strings, one per line.

The output is a textual file as well. Each line reports the distances between a string and all others. A line looks like:
```
5 0.242 0.248 0.438 0.829 0.164 0.949 ...
```
It means that the line reports distances between the string 5 and all other strings.
For instance, the distance between string 5 and string 0 is 0.242.
Remind that strings are numbered starting from 0, in the order they are arranged in the input file.
The output file is stored using Spark `saveAsTextFile`, and, as such, it consists of multiple `parts`.

## Execution
The script expects 3 command line arguments:
```
spark-submit p_distance_matrix_better.py <input_file> <output_file> <executors>
```
* **input_file**: path to the input file
* **output_file**: path where output is stored (can be HDFS)
* **executors**: number of Spark executors to launch 

