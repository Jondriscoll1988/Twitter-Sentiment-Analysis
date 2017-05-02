import json
import yahoo_finance as yf
import csv
import glob

json_list = glob.glob('/Users/jonathandriscoll/Desktop/Twitter Data/sample/01/26/*/*.json.bz2')
RDD = sc.textFile(','.join(json_list))



with open('/Users/jonathandriscoll/PycharmProjects/ProjectC/posneg.json','r') as infile:  # opens the original Loughran file
    posneg=json.load(infile)


def filters(line):
    kw_list = ['microsoft', 'Microsoft' 'msft', 'ibm', 'starbucks', 'sbux', 'nvidia', 'nvda','yahoo', 'yhoo','trump','Trump']
    return (any(word in line for word in kw_list))


"""returns a list of all tokenized words that appear in dictionary, to be used in scorekeeper THIS WORKS"""
def in_posneg(word_list):
    list=[]
    for word in word_list:
        if str(word).upper() in posneg:
            list.append(word)
    return list

"""applies scorekeeper to each tweet and returns a list of sentiment scores.  WORKS!"""
def get_sentiment(list):
    score=0
    for word in word_list:
        word=str(word).upper()
        if posneg[word]['pos'] != 0:
            score += 1
        else:
            score += -1
    return score





def historical(symbol,startdate,enddate):
    share=yf.Share(symbol)
    history=share.get_historical(startdate,enddate)
    hist_prices=[]
    for day in history:
        hist_prices.append(day.get('Close'))
    return hist_prices



