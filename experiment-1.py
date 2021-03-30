# https://www.tutorialspoint.com/python_text_processing/python_remove_stopwords.htm
# https://stackoverflow.com/questions/40287237/pyspark-dataframe-operator-is-not-in
# https://spark.apache.org/docs/latest/api/python/reference/index.html

import time

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Experiment 1").getOrCreate()

# Turn on spark-rapids
spark.conf.set('spark.rapids.sql.enabled', 'true')

# Get list of words to use
stop = list(set(stopwords.words('english')))
text_files = ['./textFiles/PrideAndPrejudice.txt', 
              './textFiles/ATaleOfTwoCities.txt', 
              './textFiles/AdventuresInWonderland.txt', 
              './textFiles/MobyDick.txt', 
              './textFiles/SherlockHolmes.txt', 
              './textFiles/Illiad.txt', 
              './textFiles/WarAndPeace.txt', 
              './textFiles/SleepyHollow.txt'
              ]

with open('./words.txt', 'w') as f2:
    for file in text_files:
        with open(file, 'r') as f:
            line = f.readline()
            while line != '':
                words = line.replace("\n", '').split(' ')
                for word in words:
                    if len(word) > 0:
                        word = word.strip('-\'\";\.,()”“?!_:_—\.’‘').lower()
                        f2.write(f'{word}\n')              
                line = f.readline()

start = time.time()

df = spark.read.text("./words.txt")
not_stop_words = df.select('value').filter(~df.value.isin(stop)).groupBy('value').count()
are_stop_words = df.select('value').filter(df.value.isin(stop)).groupBy('value').count()

# Get the 3 words that appear most frequently
not_stop_words.orderBy('count', ascending=False).show(3)

# Get the 3 words that do not appear most frequently
not_stop_words.orderBy('count', ascending=True).show(3)

# What's the max, min number of times a word appears? Average?
not_stop_words.agg({'count': 'max', 'count': 'min', 'count': 'avg'}).show()

# Get the 3 stop words that appear most frequently
are_stop_words.orderBy('count', ascending=False).show(3)

# How many times do the words "grand", "valley", "state", "university" appear?
df.select('value').filter(df.value.isin('grand', 'valley', 'state', 'university')).groupBy('value').count().orderBy('count', ascending=False).show()

# What about the words "how", "now", "brown", "cow"?
df.select('value').filter(df.value.isin('how', 'now', 'brown', 'cow')).groupBy('value').count().orderBy('count', ascending=False).show()

# Do these names appear in any of the texts?
df.select('value').filter(df.value.isin('sam', 'james', 'carl', 'joe', 'chris')).groupBy('value').count().orderBy('count', ascending=False).show()

end = time.time() - start

# Cleanup
spark.stop()

print(f'\nEnd time: {end}\n')