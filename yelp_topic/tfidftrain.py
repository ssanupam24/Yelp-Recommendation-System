import logging
import pdb
import gensim
from gensim.corpora import MmCorpus
from gensim import corpora
from pymongo import MongoClient

from settings import Settings
from sets import Set

tags_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][Settings.REVIEWS_COLLECTION]

class Corpus(object):
    def __init__(self, cursor, reviews_dictionary, corpus_path):
        self.cursor = cursor
        self.reviews_dictionary = reviews_dictionary
        self.corpus_path = corpus_path

    def __iter__(self):
        self.cursor.rewind()
        for review in self.cursor:
            yield self.reviews_dictionary.doc2bow(review["words"])

    def serialize(self):
        MmCorpus.serialize(self.corpus_path, self, id2word=self.reviews_dictionary)

        return self


class Dictionary(object):
    def __init__(self, cursor, dictionary_path):
        self.cursor = cursor
        self.dictionary_path = dictionary_path

    def build(self):
        self.cursor.rewind()
        wordlist=[]
	for review in self.cursor:
	    wordlist.append(review["words"])
        dictionary = corpora.Dictionary(wordlist)
        #dictionary.filter_extremes(keep_n=10000)
        #dictionary.compactify()
        corpora.Dictionary.save(dictionary, self.dictionary_path)
        return dictionary


class Train:
    def __init__(self):
        pass

    @staticmethod
    def run(tfid_model_path, corpus_path):
        corpus = corpora.MmCorpus(corpus_path)
	print corpus
	#pdb.set_trace()
        tfidf = gensim.models.TfidfModel(corpus)
        tfidf.save(tfid_model_path)
        return tfidf


def main():
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    corpus_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][
        Settings.CORPUS_COLLECTION]
    userList = []
    businessList = []
    fd = open("user.txt","r")
    for line in fd:
        userList.append(line.rstrip())
    fd1 = open("business.txt","r")
    for line in fd1:
        businessList.append(line.rstrip())
    
    userList=Set(userList)
    userList=list(userList)
    businessList=Set(businessList)
    businessList=list(businessList)
    user = 'ZH6JC0S9-y11sxEX6rbiBQ'
    userReviewList = corpus_collection.find({"userId":user})
    dictionary_path = "models/" + user + ".dict"
    dictionary = Dictionary(userReviewList, dictionary_path).build()
    corpus_path = "models/" + user + ".corpus"
    #Corpus(userReviewList, dictionary, corpus_path).serialize()
    userReviewList.rewind()
    wordList=[]
    for review in userReviewList:
	wordList.append(review["words"])
    print wordList
    print "################"
    userReviewList.rewind()
    corpus = [dictionary.doc2bow(review["words"]) for review in  userReviewList]
    print corpus
    #pdb.set_trace()

    # Create TF IDF model for user 
    tfid_model_path = "models/" + user + ".tfidf"
    tfidf = Train.run(tfid_model_path, corpus_path)
    print tfidf
    counter = 0 

    # For each business run above TF IDF model 
    for business in businessList:
	business_cursor = corpus_collection.find({"business":business})
	new_review_bow = []
	new_review_tfid = []
	# Extract all review of each business and run TF -IDF
	for review in business_cursor:
	    new_review_bow = dictionary.doc2bow(review["words"])
	    print new_review_bow
            print tfidf
	    counter = counter + 1
	    print (tfidf[new_review_bow])
            new_review_tfid.append(tfidf[new_review_bow])
	    if counter > 20:
	        break
	#rating = [review["ratings"] for review in business_cursor]
	#print new_review_tfid

if __name__ == '__main__':
    main()
