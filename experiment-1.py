""" Experiment #1: Fun with words

    Perform several Spark SQL queries on a large list of words, gathered across 15 books.

    The following resources were helpful when writing this program:
        * https://nvidia.github.io/spark-rapids/
        * https://www.tutorialspoint.com/python_text_processing/python_remove_stopwords.htm
        * https://stackoverflow.com/questions/40287237/pyspark-dataframe-operator-is-not-in
        * https://spark.apache.org/docs/latest/api/python/reference/index.html
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
spark = SparkSession.builder.appName("Experiment 1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Turn on spark-rapids
spark.conf.set('spark.rapids.sql.enabled', 'true')

# Get list of stop words to use & generate data
stop = list(set(stopwords.words('english')))
generate_word_file()

query_times = []
total_start = time.time()

# Read in generated text file, creating a DataFrame with a 'value' column, and group by 'value', obtaining a count for each word
df = spark.read.text("./words.txt")
not_stop_words = df.select('value').filter(~df.value.isin(stop)).groupBy('value').count()
are_stop_words = df.select('value').filter(df.value.isin(stop)).groupBy('value').count()

print("Query #1: get 3 words that appear most frequently and are not stop words")
query_start = time.time()
not_stop_words.orderBy('count', ascending=False).show(3)
query_times.append(time.time() - query_start)

print("Query #2: get 3 words that do not appear most frequently and are not stop words")
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

print("Query #7: do any of the following names appear? Sam, James, Carl, Joe, Chris")
query_start = time.time()
df.select('value').filter(df.value.isin('sam', 'james', 'carl', 'joe', 'chris')).groupBy('value').count().orderBy('count', ascending=False).show()
query_times.append(time.time() - query_start)

print("Query #8: do these words appear? 'savior', 'glory', 'mother-in-law', 'father-in-law', 'just-in-case'?")
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

total_end = time.time() - total_start

# Cleanup
spark.stop()

print("----------------------------")
for i, query_t in enumerate(query_times, start=1):
    print(f"Query {i} time: {query_t}s")

print(f'\nTotal time to read in file & run all queries: {total_end}s\n')
print("----------------------------")
