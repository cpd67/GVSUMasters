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
text_files = ['./PrideAndPrejudice.txt', './ATaleOfTwoCities.txt', './AdventuresInWonderland.txt', './MobyDick.txt', './SherlockHolmes.txt', './Illiad.txt', './WarAndPeace.txt', './SleepyHollow.txt']
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

# Character analysis - Experiment 2
# Words that do not start with 'a'
df.select('value').filter(~df.value.like('a%')).collect()

# Words that contain 'ab'
df.select('value').filter(df.value.like('%ab%')).collect()

# Words that contain 'c'
df.select('value').filter(df.value.like('%c%')).collect()

# Words that have 1 or more 'a's
df.select('value').filter(df.value.rlike('*[a]+*$')).collect()


end = time.time() - start

# Cleanup
spark.stop()

print(f'\nEnd time: {end}\n')
