from pyspark.sql import SQLContext, explode
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import Tokenizer, SQLTransformer
import glob

sc = SparkContext()
sq = SQLContext()

"""will be following Microsoft (MSFT), IBM (IBM), Starbucks (SBUX), Nividia (NVDA), and Yahoo (YHOO) stocks"""

d = '/Users/jonathandriscoll/PycharmProjects/Project_C/posneg.json'
zips = '/Users/jonathandriscoll/Desktop/Twitter Data/sample/01/26/'
day1_json_list = glob.glob('/Users/jonathandriscoll/Desktop/Twitter Data/sample/01/26/*/*.json.bz2')
day1_RDD = sc.textFile(','.join(day1_json_list))

def filters(line):
    return (any(word in line.text for word in kw_list))

filtered=day1_RDD.filter(filters)

"""creates RDD """
my_RDD_strings = sc.textFile(zips)  # creates an RDD object from a file or group of files
RDD_collect = my_RDD_strings.collect()  # collect() creates a list and reads ENTIRE RDD to memory.  be mindful of file sizes.
dict = sc.textFile(d) #RDD object of sentiment dictionary

"""dataframe manipulations"""
df = spark.read.json(my_RDD_strings)  # creates a pyspark dataframe of twitter data
sentiment_dict = spark.read.json(dict) # creates pyspark dataframe of sentiment dictionary
english_tweets = df.select(df.created_at, df.text).filter(df.lang == 'en')  # selects the appropriate keys from dataframe.  filters by language, only returns tweets in english.  Will be updated as model improves.
rdd_eng=english_tweets.rdd
rdd_comp=rdd_eng.flatMap(mention_it)
tokenizer = Tokenizer(inputCol='text', outputCol='words')
token = tokenizer.transform(english_tweets)  # separates every tweet into word tokens
tweet_tokens = token.select(token.words)  # only the column of individual word tokens
tweet_tokens_rdd=tweet_tokens.rdd
token_explode=tweet_tokens.select(explode(tweet_tokens.words).alias('wordlist'))
explode_rdd=token_explode.rdd


mention_it(str(tweet_tokens_rdd.collect()))  #kinda works


"""one way of filtering tweets by company, not very elegant but works fine"""
SBUX= my_RDD_strings.filter(lambda element: 'Starbucks' in element).collect()
MSFT=my_RDD_strings.filter(lambda element: 'Microsoft' in element).collect()
IBM=my_RDD_strings.filter(lambda element: 'IBM' in element).collect()
NVDA=my_RDD_strings.filter(lambda element: 'Nvidia' in element).collect()
YHOO=my_RDD_strings.filter(lambda element: 'Yahoo' in element).collect()
trump=my_RDD_strings.filter(lambda element: 'trump' in element).collect()

trump=my_RDD_strings.filter(lambda element: element[0]==text).collect()

wordcount=tweet_tokens_rdd.map(lambda word: (word,1))\
    .reduceByKey(lambda a, b: a+b)\
    .map(lambda x: (x[1], x[0])).sortByKey(False)
maptokens=tweet_tokens_rdd.map(lambda word: (word,1))
redtokens=maptokens.reduceByKey(lambda a,b: a+b).collect()

def mention_it(list):
    twitlist=[]
    kw_list = ['microsoft', 'msft', 'ibm', 'starbucks', 'sbux', 'nvidia', 'nvda','yahoo', 'yhoo','trump','lynch']
    for sublist in str(list):
        if any(kw_list):
            twitlist.append(sublist)
    return twitlist

