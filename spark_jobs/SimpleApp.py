from __future__ import print_function

import sys
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv) != 3:
    print("Usage: SimpleApp.py <input_file> <output_file>", file=sys.stderr)
    exit(-1)

  sc = SparkContext()

  input_file = sys.argv[1] 
  output_dir = sys.argv[2]

  # Load the text file into an RDD
  lines = sc.textFile(input_file)

  # Split each line into words using the split() method
  words = lines.flatMap(lambda line: line.split())

  # Map each word to a key-value pair with the word as the key and 1 as the value
  word_counts = words.map(lambda word: (word, 1))

  # Sum the counts for each word using the reduceByKey() method
  word_count_totals = word_counts.reduceByKey(lambda x, y: x + y)

  # Repartition the RDD into a single partition
  word_count_totals = word_count_totals.coalesce(1)

  # Save the word count totals to a text file
  word_count_totals.saveAsTextFile(output_dir)
