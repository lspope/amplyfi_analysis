import pymongo
import json
import os
import nlpActions
import dns

from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from pprint import pprint

# Get creditials 
db_username = "do_not_checkin_secrets"
db_pass = "do_not_checkin_secrets"
db_clustername = "do_not_checkin_secrets"

creds_str = "mongodb+srv://" + db_username + ":" +  db_pass +"@"+ db_clustername + ".zqqan.mongodb.net/<demo>?retryWrites=true&w=majority"
mongo_client = MongoClient(creds_str)
articles_db = mongo_client.demo
articles_collection = articles_db["article_analysis"]


def get_standardized_source(source):
    source = source.lstrip()
    source = source.strip()
    return source.lower()


def populate_articles_db(path_to_articles):
    write_batch = []
    original_count = articles_collection.count_documents({})
    print(path_to_articles)
    filenames = [filename for filename in os.listdir(path_to_articles) if filename.endswith(".json")]


    for filename in filenames:
        try:
            filepath = os.path.join(path_to_articles, filename)
            with open(filepath, encoding="utf-8") as orig_json:
                tmp = json.load(orig_json)      # Read the JSON file
                orig_json.close()               # Close the JSON file
            
            # get the derived features

            # meta data
            tmp["sourceStandardized"] = get_standardized_source(tmp["source"])
            tmp["wordCount"] = len(tmp["body"].split())
            tmp["publishYear"] = int(tmp["publishDate"][0:4])
            tmp["publishMonth"] = int(tmp["publishDate"][5:7])

            # get entity info (count and entities listed by type)
            entities_info_dict = nlpActions.get_target_entities(tmp["body"])
            tmp["entityMentions"] = entities_info_dict["entityMentions"]
            tmp["person"] = entities_info_dict["PERSON"]
            tmp["norp"] = entities_info_dict["NORP"]
            tmp["fac"]= entities_info_dict["FAC"]
            tmp["org"] = entities_info_dict["ORG"]
            tmp["gpe"] = entities_info_dict["GPE"]
            tmp["loc"] = entities_info_dict["LOC"]
            tmp["product"] = entities_info_dict["PRODUCT"]
            tmp["event"] = entities_info_dict["EVENT"]
            tmp["law"] = entities_info_dict["LAW"]

            write_batch.append(InsertOne(tmp))

        except Exception as e:
            print(f"Something went wrong with reading and/or NLP processing article file {filepath}.")
            print(e)

    print(f"About to bulk write {len(write_batch)} docs")

    if len(write_batch) > 0:
        try:
            result = articles_collection.bulk_write(write_batch)
            print(f"bulk write ack?: {result.acknowledged}")
            print(f"bulk write insert count: {result.inserted_count}")
        except BulkWriteError as bwe:
            pprint(bwe.details)

    new_count = articles_collection.count_documents({})
    print(f"added {new_count-original_count} articles to collection")


if __name__ == "__main__":
    my_path = "/home/user/Flatiron/post_work/amplyfi_analysis/data/news_articles/load777"
    path_to_articles =  os.path.abspath(my_path)
    populate_articles_db(path_to_articles)
    print("......... Goodbye!")
