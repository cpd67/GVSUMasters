# https://www.tutorialspoint.com/python_text_processing/python_remove_stopwords.htm
# https://stackoverflow.com/questions/40287237/pyspark-dataframe-operator-is-not-in
# https://spark.apache.org/docs/latest/api/python/reference/index.html

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

# Get list of words to use
stop = list(set(stopwords.words('english')))
generate_word_file()

df = spark.read.text("./words.txt")

# Character analysis - Experiment 2
# Words that do not start with 'a'
df.select('value').filter(~df.value.like('a%')).show()

# Words that contain 'ab'
df.select('value').filter(df.value.like('%ab%')).show()

# Words that contain 'c'
df.select('value').filter(df.value.like('%c%')).show()

# TODO: More character analysis

end = time.time() - start

# Cleanup
spark.stop()

print(f'\nEnd time: {end}\n')
