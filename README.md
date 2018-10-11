# Spark Distance Matrix Calculator

This piece of code computes a distance matrix for a set of input strings. It uses Apache Spark to achieve parallelism.

It uses the normalized version of the Levenshtein distance, but you can easily [change the code](https://github.com/marty90/spark-distance-matrix/blob/8b76c1348435352d4e20c589fb27dd0d81556bfa/spark_distance_matrix.py#L23) to include your favorite distance function.

For any information or request write to [martino.trevisan@polito.it](mailto:martino.trevisan@polito.it).

## Prerequisites
You need (a cluster with) Apache Spark, and Python.
The only needed library is python-Levenshtein. You can easily install it with:
```
pip install python-Levenshtein
```
Remind that if you have a cluster, `python-Levenshtein` should be installed on all nodes.

## Input and output format
The script expects as input a single file containing the target strings, one per line. Use you own data, or download public datasets (like [this](https://www.kaggle.com/simsek/openphishcom-phishing-urls-on-oct-2-2017#dataset.csv), [this](https://www.kaggle.com/teseract/urldataset) or [this](http://machinelearning.inginf.units.it/data-and-tools/hidden-fraudulent-urls-dataset)).

The output is a textual file as well. Each line reports the distances between a string and all others. A line looks like:
```
5 0.242 0.248 0.438 0.829 0.164 0.949 ...
```
It means that this line reports distances between the string 5 and all other strings.
For instance, the distance between string 5 and string 0 is 0.242.
Remind that strings are numbered starting from 0, in the order they are arranged in the input file.
The output file is stored using Spark `saveAsTextFile`, and, as such, it consists of multiple `parts`.

## Execution
The script expects 3 command line arguments:
```
spark-submit spark_distance_matrix.py <input_file> <output_file> <executors>
```
* **input_file**: path to the input file
* **output_file**: path where output is stored (can be HDFS)
* **executors**: number of Spark executors to launch 

