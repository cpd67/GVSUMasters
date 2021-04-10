""" Experiment #2: Character Frequency Analysis

    Perform several Spark SQL queries on a large list of words, gathered across 15 books.

    The following resources were helpful when writing this program:
        * https://nvidia.github.io/spark-rapids/
        * https://spark.apache.org/docs/latest/api/python/reference/index.html
        * https://www.sqlservertutorial.net/sql-server-basics/sql-server-like/
        * http://www.nltk.org/api/nltk.html
        * https://www.tutorialspoint.com/python_text_processing/python_remove_stopwords.htm
        * https://gist.github.com/sebleier/554280
        * https://stackoverflow.com/questions/28189408/how-to-reduce-the-verbosity-of-sparks-runtime-output
        * https://spark.apache.org/docs/latest/sql-getting-started.html
        * https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/readwriter.html
"""
import time

import nltk
# Download and use the stopwords package
nltk.download('stopwords')
from nltk.corpus import stopwords
from pyspark.sql import SparkSession

from word import generate_word_file


# Create a SparkSession instance, the entry point for all spark functionality
spark = SparkSession.builder.appName("Experiment 2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Turn on spark-rapids
spark.conf.set('spark.rapids.sql.enabled', 'true')

# Get list of words to use & generate data to use
stop = list(set(stopwords.words('english')))
generate_word_file()

query_times = []
total_start = time.time()

# Read in generated text file, creating a DataFrame with a 'value' column
df = spark.read.text("./words.txt")

print("Query #1: words that do not start with 'a'")
query_start = time.time()
query = df.select('value').filter(~df.value.like('a%')).distinct()
query.show(10)
word_count = query.count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #2: words that do start with 'a'")
query_start = time.time()
query = df.select('value').filter(df.value.like('a%')).distinct()
query.show(10)
word_count = query.count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #3: words that contain 'ab'")
query_start = time.time()
query = df.select('value').filter(df.value.like('%ab%')).distinct()
query.show(10)
word_count = query.count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #4: words that contain 'c'")
query_start = time.time()
query = df.select('value').filter(df.value.like('%c%')).distinct()
query.show(10)
word_count = query.count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #5: words that contain 'f'")
query_start = time.time()
query = df.select('value').filter(df.value.like('%f%')).distinct()
query.show(10)
word_count = query.count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #6: words that end in 'z'")
query_start = time.time()
query = df.select('value').filter(df.value.like('%z')).distinct()
query.show(10)
word_count = query.count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #7: 8-letter words that end in 'ing'")
query_start = time.time()
query = df.select('value').filter(df.value.like('_____ing')).distinct()
query.show(10)
word_count = query.count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #8: 4-letter words that start with y")
query_start = time.time()
query = df.select('value').filter(df.value.like('y___')).distinct()
query.show(10)
word_count = query.count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #9: Words where the third character is 'i'")
query_start = time.time()
query = df.select('value').filter(df.value.like('__i%')).distinct()
query.show(10)
word_count = query.count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #10: words that start with 'g', and where the fifth character is an 'e'")
query_start = time.time()
query = df.select('value').filter(df.value.like('g___e%')).distinct()
query.show(10)
word_count = query.count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

total_end = time.time() - total_start

# Cleanup
spark.stop()

print("----------------------------")
for i, query_t in enumerate(query_times, start=1):
    print(f"Query {i} time: {query_t}s")

print(f'\nTotal time to read in file & run all queries: {total_end}s\n')
print("----------------------------")
