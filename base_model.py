import json
import pickle
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col
import yahoo_finance as yf
import matplotlib as plt
sc = SparkContext()
sq = SQLContext(sc)

kw_list = ['Microsoft', 'MSFT', 'IBM', 'SBUX', 'Starbucks', 'NVDA', 'Nvidia', 'YHOO', 'Yahoo']
with open('/Users/jonathandriscoll/PycharmProjects/ProjectC/posneg.json','r') as infile:  # opens the original Loughran file
    posneg=json.load(infile)



json_list = glob.glob('/Users/jonathandriscoll/Desktop/Twitter Data/sample/01/*')
#SBUX26= df.where(col('created_at').like("%Jan 26 %")).where(col('text').like('%starbucks%'))



def filters(line):
    kw_list = ['microsoft', 'Microsoft' 'msft', 'ibm', 'starbucks', 'sbux', 'nvidia', 'nvda','yahoo', 'yhoo','trump','Trump']
    return (any(word in line for word in kw_list))



"""returns the ratio of specific company mentions compared to total mentions of all companies"""
def ratio(dict):
    ratio_dict={}
    for key in dict:
        if dict.get(key) is None:
            dict[key]=0
    total_mentions= sum(dict.get(key) for key in dict)
    for key in dict:
        ratio_dict[key] = dict.get(key)/total_mentions
    return ratio_dict

"""in_posneg finds all words in a tweet that appear in the posneg sentiment dictionary.  returns them as a list"""

def in_posneg(word_list):
    list=[]
    for word in word_list:
        if str(word).upper() in posneg:
            list.append(word)
    return list

"""applies scorekeeper to each tweet and returns a list of sentiment scores.  WORKS"""
def get_sentiment(list):
    in_dict=in_posneg(list)
    score=0
    for word in in_dict:
        word=word.upper()
        if posneg[word]['pos'] != 0:
            score += 1
        elif posneg[word]['neg'] != 0:
            score += -1
        else:
            score +=0
    return score

"""gets closing prices from yahoo finance for specific companies"""

def historical(symbol,startdate,enddate):
    share=yf.Share(symbol)
    history=share.get_historical(startdate,enddate)
    hist_prices=[]
    for day in history:
        hist_prices.append(day.get('Close'))
    return hist_prices

"""creates a dict of keywords and their counts."""

def day_parse(filedir):
    count = 1
    day_counts = {}
    for folder in filedir:
        day = folder + '/*/*.json.bz2'
        df = spark.read.json(day)
        df = df.filter(df['lang'] == 'en')
        df = df.select(['created_at', 'text'])
        #df=tokenizer.transform(df)
        filtered_RDD = df.rdd
        filtered_words = filtered_RDD.flatMap(lambda x: x.text.split(' '))\
                .map(lambda x: (x,1)) \
                .reduceByKey(lambda x,y: x+y)
        filtered_words = filtered_words.collect()   ###Filtered
        filtered_words = dict(filtered_words)
        kw_count = [filtered_words.get(key) for key in kw_list]
        count_dict =dict(zip(kw_list, kw_count))
        day_counts[str(count)] = count_dict
        count += 1
        return day_counts


def buysell(dict):












sentiments= filtered_RDD.flatMap(lambda x: x.text.split(' '))\





