""" Experiment #1: Fun with words

    Perform several spark queries on a large list of words, gathered across 15 books.

    The following resources were helpful when writing this program:
        * https://www.tutorialspoint.com/python_text_processing/python_remove_stopwords.htm
        * https://stackoverflow.com/questions/40287237/pyspark-dataframe-operator-is-not-in
        * https://spark.apache.org/docs/latest/api/python/reference/index.html
"""

import time

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from pyspark.sql import SparkSession

from word import generate_word_file


spark = SparkSession.builder.appName("Experiment 1").getOrCreate()
# There's also a log4j.properties file that controls the logging output of spark globally: https://stackoverflow.com/questions/28189408/how-to-reduce-the-verbosity-of-sparks-runtime-output
spark.sparkContext.setLogLevel("ERROR")

# Turn on spark-rapids
spark.conf.set('spark.rapids.sql.enabled', 'true')

start = time.time()

# Get list of stop words to use & generate data to use
stop = list(set(stopwords.words('english')))
generate_word_file()

query_times = []

df = spark.read.text("./words.txt")
not_stop_words = df.select('value').filter(~df.value.isin(stop)).groupBy('value').count()
are_stop_words = df.select('value').filter(df.value.isin(stop)).groupBy('value').count()

print("Query #1: get 3 words that appear most frequently")
query_start = time.time()
not_stop_words.orderBy('count', ascending=False).show(3)
query_times.append(time.time() - query_start)

print("Query #2: get 3 words that do not appear most frequently")
query_start = time.time()
not_stop_words.orderBy('count', ascending=True).show(3)
query_times.append(time.time() - query_start)

print("Query #3: what is the max, min, average number of times a word appears?")
query_start = time.time()
not_stop_words.agg({'count': 'max'}).show()
not_stop_words.agg({'count': 'min'}).show()
not_stop_words.agg({'count': 'avg'}).show()
query_times.append(time.time() - query_start)

print("Query #4: get 3 stop words that appear most frequently")
query_start = time.time()
are_stop_words.orderBy('count', ascending=False).show(3)
query_times.append(time.time() - query_start)

print("Query #5: how many times do the words 'grand', 'valley', 'state', 'university' appear?")
query_start = time.time()
df.select('value').filter(df.value.isin('grand', 'valley', 'state', 'university')).groupBy('value').count().orderBy('count', ascending=False).show()
query_times.append(time.time() - query_start)

print("Query #6: how many times do the words 'how', 'now', 'brown', 'cow' appear?")
query_start = time.time()
df.select('value').filter(df.value.isin('how', 'now', 'brown', 'cow')).groupBy('value').count().orderBy('count', ascending=False).show()
query_times.append(time.time() - query_start)

print("Query #7: do any of the following names appear? Sam, James, Joe, Chris")
query_start = time.time()
df.select('value').filter(df.value.isin('sam', 'james', 'carl', 'joe', 'chris')).groupBy('value').count().orderBy('count', ascending=False).show()
query_times.append(time.time() - query_start)

print("Query #8: do these words appear? 'mother-in-law', 'father-in-law', 'just-in-case'?")
query_start = time.time()
df.select('value').filter(df.value.isin('savior', 'glory', 'mother-in-law', 'father-in-law', 'just-in-case')).groupBy('value').count().orderBy('count', ascending=False).show()
query_times.append(time.time() - query_start)

print("Query #9: do these words appear? 'apple', 'orange', 'pear', 'blueberry', 'grape', 'cherry'?")
query_start = time.time()
df.select('value').filter(df.value.isin('apple', 'orange', 'pear', 'blueberry', 'grape', 'cherry')).groupBy('value').count().orderBy('count', ascending=False).show()
query_times.append(time.time() - query_start)

print("Query #10: how many times do the words 'cat', 'dog', 'bear', 'cow', 'lizard', 'hamster', 'peacock' appear?")
query_start = time.time()
df.select('value').filter(df.value.isin('cat', 'dog', 'bear', 'cow', 'lizard', 'hamster', 'peacock')).groupBy('value').count().orderBy('count', ascending=False).show()
query_times.append(time.time() - query_start)

end = time.time() - start

# Cleanup
spark.stop()

print("----------------------------")
for i, query_t in enumerate(query_times, start=1):
    print(f"Query {i} time: {query_t}s")

print(f'\nProgram run time: {end}s\n')
print("----------------------------")
