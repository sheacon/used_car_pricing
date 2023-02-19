
# bash: cd ~/used_car_pricing/spark-slurm
# bash: pyspark

#sc = SparkContext()

input_file = "README.md"
output_dir = "wordcount_results"

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

#sc.stop()
