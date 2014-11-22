import logging
from pymongo import MongoClient
from settings import Settings




tags_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][Settings.REVIEWS_COLLECTION]

def main():
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    corpus_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][
        Settings.CORPUS_COLLECTION]
    userList = {}
    businessList = {}
    reviews_cursor = corpus_collection.find()
    for review in reviews_cursor:
	#print review["rating"]
	if review["rating"] in [5, 4]:
		if review["business"] in userList[review["userId"]]:
		    continue
		else:
		    userList[review["userId"]] = review["business"]
		if review["business"] in businessList:
		    businessList[review["business"]] = businessList[review["business"]] + 1
		else:
		    businessList[review["business"]] = 1
	print userList


if __name__ == '__main__':
    main()

