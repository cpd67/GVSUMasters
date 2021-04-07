""" Experiment #2: Character Frequency Analysis

    Perform several spark queries on a large list of words, gathered across 15 books.

    The following resources were helpful when writing this program:
        * https://spark.apache.org/docs/latest/api/python/reference/index.html
        * https://www.sqlservertutorial.net/sql-server-basics/sql-server-like/
"""
import time

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from pyspark.sql import SparkSession

from word import generate_word_file


spark = SparkSession.builder.appName("Experiment 2").getOrCreate()
# There's also a log4j.properties file that controls the logging output of spark globally: https://stackoverflow.com/questions/28189408/how-to-reduce-the-verbosity-of-sparks-runtime-output
spark.sparkContext.setLogLevel("ERROR")

# Turn on spark-rapids
spark.conf.set('spark.rapids.sql.enabled', 'true')

# Get list of words to use & generate data to use
stop = list(set(stopwords.words('english')))
generate_word_file()

query_times = []

start = time.time()

df = spark.read.text("./words.txt")

print("Query #1: words that do not start with 'a'")
query_start = time.time()
df.select('value').filter(~df.value.like('a%')).distinct().show(10)
word_count = df.select('value').filter(~df.value.like('a%')).distinct().count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #2: words that do start with 'a'")
query_start = time.time()
df.select('value').filter(df.value.like('a%')).distinct().show(10)
word_count = df.select('value').filter(df.value.like('a%')).distinct().count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #3: words that contain 'ab'")
query_start = time.time()
df.select('value').filter(df.value.like('%ab%')).distinct().show(10)
word_count = df.select('value').filter(df.value.like('%ab%')).distinct().count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #4: words that contain 'c'")
query_start = time.time()
df.select('value').filter(df.value.like('%c%')).distinct().show(10)
word_count = df.select('value').filter(df.value.like('%c%')).distinct().count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #5: words that contain 'f'")
query_start = time.time()
df.select('value').filter(df.value.like('%f%')).distinct().show(10)
word_count = df.select('value').filter(df.value.like('%f%')).distinct().count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #6: words that end in 'z'")
query_start = time.time()
df.select('value').filter(df.value.like('%z')).distinct().show(10)
word_count = df.select('value').filter(df.value.like('%z')).distinct().count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #7: 8-letter words that end in 'ing'")
query_start = time.time()
df.select('value').filter(df.value.like('_____ing')).distinct().show(10)
word_count = df.select('value').filter(df.value.like('_____ing')).distinct().count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #8: 4-letter words that start with y")
query_start = time.time()
df.select('value').filter(df.value.like('y___')).distinct().show(10)
word_count = df.select('value').filter(df.value.like('y___')).distinct().count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #9: Words where the third character is 'i'")
query_start = time.time()
df.select('value').filter(df.value.like('__i%')).distinct().show(10)
word_count = df.select('value').filter(df.value.like('__i%')).distinct().count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

print("Query #10: words that start with 'g', and where the fifth character is a 'e'")
query_start = time.time()
df.select('value').filter(df.value.like('g___e%')).distinct().show(10)
word_count = df.select('value').filter(df.value.like('g___e%')).distinct().count()
query_times.append(time.time() - query_start)
print(f"Number of words: {word_count}")

end = time.time() - start

# Cleanup
spark.stop()

print("----------------------------")
for i, query_t in enumerate(query_times, start=1):
    print(f"Query {i} time: {query_t}s")

print(f'\nProgram run time: {end}s\n')
print("----------------------------")
