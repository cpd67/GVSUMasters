""" Perform several Spark SQL queries on a large list of words, gathered across 15 books.

    The following resources were helpful when writing this program:
        * https://nvidia.github.io/spark-rapids/
        * https://www.tutorialspoint.com/python_text_processing/python_remove_stopwords.htm
        * https://stackoverflow.com/questions/40287237/pyspark-dataframe-operator-is-not-in
        * https://spark.apache.org/docs/latest/api/python/reference/index.html
        * http://www.nltk.org/api/nltk.html
        * https://gist.github.com/sebleier/554280
        * https://stackoverflow.com/questions/28189408/how-to-reduce-the-verbosity-of-sparks-runtime-output
        * https://spark.apache.org/docs/latest/sql-getting-started.html
        * https://www.sqlservertutorial.net/sql-server-basics/sql-server-like/
        * https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/readwriter.html
"""

import time

from nltk.corpus import stopwords
from pyspark.sql import SparkSession


def experiment_1(rapids_off=False):
    """ Perform queries to analyze certain characteristics of the books, such as how many times a certain word appears,
        what are 3 words that appear most frequently (that are not stop words), etc.

        rapids_off: boolean kwarg to control when to disable spark-rapids at runtime.
    """

    print("Experiment #1: Fun with words")

    # Create a SparkSession instance, the entry point for all spark functionality
    spark_experiment_1 = SparkSession.builder.appName("Experiment 1").getOrCreate()
    spark_experiment_1.sparkContext.setLogLevel("ERROR")

    if rapids_off:
        spark_experiment_1.conf.set('spark.rapids.sql.enabled', 'false')

    # Get list of stop words to use
    stop = list(set(stopwords.words('english')))

    query_times = []

    # Read in generated text file, creating a DataFrame with a 'value' column, and group by 'value', obtaining a count for each word
    df = spark_experiment_1.read.text("./words.txt")
    not_stop_words = df.select('value').filter(~df.value.isin(stop)).groupBy('value').count()
    are_stop_words = df.select('value').filter(df.value.isin(stop)).groupBy('value').count()

    total_start = time.time()

    print("Query #1: get 3 words that appear most frequently and are not stop words")
    query_start = time.time()
    not_stop_words.orderBy('count', ascending=False).show(3)
    query_times.append(time.time() - query_start)

    print("Query #2: get 3 words that do not appear most frequently and are not stop words")
    query_start = time.time()
    not_stop_words.orderBy('count', ascending=True).show(3)
    query_times.append(time.time() - query_start)

    print("Query #3: what is the max number of times a word appears?")
    query_start = time.time()
    not_stop_words.agg({'count': 'max'}).show()
    query_times.append(time.time() - query_start)

    print("Query #4: what is the min number of times a word appears?")
    query_start = time.time()
    not_stop_words.agg({'count': 'min'}).show()
    query_times.append(time.time() - query_start)

    print("Query #5: what is the average number of times a word appears?")
    query_start = time.time()
    not_stop_words.agg({'count': 'avg'}).show()
    query_times.append(time.time() - query_start)

    print("Query #6: get 3 stop words that appear most frequently")
    query_start = time.time()
    are_stop_words.orderBy('count', ascending=False).show(3)
    query_times.append(time.time() - query_start)

    print("Query #7: how many times do the words 'grand', 'valley', 'state', 'university' appear?")
    query_start = time.time()
    df.select('value').filter(df.value.isin('grand', 'valley', 'state', 'university')).groupBy('value').count().orderBy('count', ascending=False).show()
    query_times.append(time.time() - query_start)

    print("Query #8: how many times do the words 'how', 'now', 'brown', 'cow' appear?")
    query_start = time.time()
    df.select('value').filter(df.value.isin('how', 'now', 'brown', 'cow')).groupBy('value').count().orderBy('count', ascending=False).show()
    query_times.append(time.time() - query_start)

    print("Query #9: do any of the following names appear? Sam, James, Carl, Joe, Chris")
    query_start = time.time()
    df.select('value').filter(df.value.isin('sam', 'james', 'carl', 'joe', 'chris')).groupBy('value').count().orderBy('count', ascending=False).show()
    query_times.append(time.time() - query_start)

    print("Query #10: do these words appear? 'savior', 'glory', 'mother-in-law', 'father-in-law', 'just-in-case'?")
    query_start = time.time()
    df.select('value').filter(df.value.isin('savior', 'glory', 'mother-in-law', 'father-in-law', 'just-in-case')).groupBy('value').count().orderBy('count', ascending=False).show()
    query_times.append(time.time() - query_start)

    print("Query #11: do these words appear? 'apple', 'orange', 'pear', 'blueberry', 'grape', 'cherry'?")
    query_start = time.time()
    df.select('value').filter(df.value.isin('apple', 'orange', 'pear', 'blueberry', 'grape', 'cherry')).groupBy('value').count().orderBy('count', ascending=False).show()
    query_times.append(time.time() - query_start)

    print("Query #12: how many times do the words 'cat', 'dog', 'bear', 'cow', 'lizard', 'hamster', 'peacock' appear?")
    query_start = time.time()
    df.select('value').filter(df.value.isin('cat', 'dog', 'bear', 'cow', 'lizard', 'hamster', 'peacock')).groupBy('value').count().orderBy('count', ascending=False).show()
    query_times.append(time.time() - query_start)

    total_end = time.time() - total_start

    # Cleanup
    spark_experiment_1.stop()

    print("----------------------------")
    for i, query_t in enumerate(query_times, start=1):
        print(f"Query {i} time: {query_t}s")

    print(f'\nTotal time to run all queries: {total_end}s\n')
    print("----------------------------")


def experiment_2(rapids_off=False):
    """ Perform character frequency analysis on our list of words. 

        rapids_off: boolean kwarg to control when to disable spark-rapids at runtime.
    """

    print("Experiment #2: Character Frequency Analysis")

    # Create a SparkSession instance, the entry point for all spark functionality
    spark_experiment_2 = SparkSession.builder.appName("Experiment 2").getOrCreate()
    spark_experiment_2.sparkContext.setLogLevel("ERROR")

    if rapids_off:
        spark_experiment_2.conf.set('spark.rapids.sql.enabled', 'false')

    # Get list of stop words to use
    stop = list(set(stopwords.words('english')))

    query_times = []

    # Read in generated text file, creating a DataFrame with a 'value' column
    df = spark_experiment_2.read.text("./words.txt")

    total_start = time.time()

    print("Queries #1 & #2: words that do not start with 'a' & how many there are")
    query_start = time.time()
    df.select('value').filter(~df.value.like('a%')).distinct().show()
    query_times.append(time.time() - query_start)

    query_start = time.time()
    word_count = df.select('value').filter(~df.value.like('a%')).distinct().count()
    query_times.append(time.time() - query_start)
    print(f"Number of words: {word_count}")

    print("Queries #3 & #4: words that do start with 'a' & how many there are")
    query_start = time.time()
    df.select('value').filter(df.value.like('a%')).distinct().show()
    query_times.append(time.time() - query_start)

    query_start = time.time()
    word_count = df.select('value').filter(df.value.like('a%')).distinct().count()
    query_times.append(time.time() - query_start)
    print(f"Number of words: {word_count}")

    print("Queries #5 & #6: words that contain 'ab'")
    query_start = time.time()
    query = df.select('value').filter(df.value.like('%ab%')).distinct().show()
    query_times.append(time.time() - query_start)

    query_start = time.time()
    word_count = df.select('value').filter(df.value.like('%ab%')).distinct().count()
    query_times.append(time.time() - query_start)
    print(f"Number of words: {word_count}")

    print("Queries #7 & #8: words that contain 'c'")
    query_start = time.time()
    df.select('value').filter(df.value.like('%c%')).distinct().show()
    query_times.append(time.time() - query_start)

    query_start = time.time()
    word_count = df.select('value').filter(df.value.like('%c%')).distinct().count()
    query_times.append(time.time() - query_start)
    print(f"Number of words: {word_count}")

    print("Queries #9 & #10: words that contain 'f'")
    query_start = time.time()
    query = df.select('value').filter(df.value.like('%f%')).distinct().show()
    query_times.append(time.time() - query_start)

    query_start = time.time()
    word_count = df.select('value').filter(df.value.like('%f%')).distinct().count()
    query_times.append(time.time() - query_start)
    print(f"Number of words: {word_count}")

    print("Queries #11 & #12: words that end in 'z'")
    query_start = time.time()
    query = df.select('value').filter(df.value.like('%z')).distinct().show()
    query_times.append(time.time() - query_start)

    query_start = time.time()
    word_count = df.select('value').filter(df.value.like('%z')).distinct().count()
    query_times.append(time.time() - query_start)
    print(f"Number of words: {word_count}")

    print("Queries #13 & #14: 8-letter words that end in 'ing'")
    query_start = time.time()
    query = df.select('value').filter(df.value.like('_____ing')).distinct().show()
    query_times.append(time.time() - query_start)

    query_start = time.time()
    word_count = df.select('value').filter(df.value.like('_____ing')).distinct().count()
    query_times.append(time.time() - query_start)
    print(f"Number of words: {word_count}")

    print("Queries #15 & #16: 4-letter words that start with y")
    query_start = time.time()
    query = df.select('value').filter(df.value.like('y___')).distinct().show()
    query_times.append(time.time() - query_start)

    query_start = time.time()
    word_count = df.select('value').filter(df.value.like('y___')).distinct().count()
    query_times.append(time.time() - query_start)
    print(f"Number of words: {word_count}")

    print("Queries ##17 & #18: Words where the third character is 'i'")
    query_start = time.time()
    query = df.select('value').filter(df.value.like('__i%')).distinct().show()
    query_times.append(time.time() - query_start)

    query_start = time.time()
    word_count = df.select('value').filter(df.value.like('__i%')).distinct().count()
    query_times.append(time.time() - query_start)
    print(f"Number of words: {word_count}")

    print("Queries #19 & #20: words that start with 'g', and where the fifth character is an 'e'")
    query_start = time.time()
    query = df.select('value').filter(df.value.like('g___e%')).distinct().show()
    query_times.append(time.time() - query_start)

    query_start = time.time()
    word_count = df.select('value').filter(df.value.like('g___e%')).distinct().count()
    query_times.append(time.time() - query_start)
    print(f"Number of words: {word_count}")

    total_end = time.time() - total_start

    # Cleanup
    spark_experiment_2.stop()

    print("----------------------------")
    for i, query_t in enumerate(query_times, start=1):
        print(f"Query {i} time: {query_t}s")

    print(f'\nTotal time to run all queries: {total_end}s\n')
    print("----------------------------")
