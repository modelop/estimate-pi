from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession

# modelop.init
def init():
    print("Begin function...", flush=True)

    global SPARK
    SPARK = SparkSession.builder.appName("PythonPi").getOrCreate()
    print("Spark variable:", SPARK, flush=True)

# modelop.score
def score(external_inputs: List, external_outputs: List, external_model_assets: List):
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_):
            x = random() * 2 - 1
            y = random() * 2 - 1
            return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = SPARK.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))

    SPARK.stop()