import pymongo
import json
import os
import re
import numpy as np
import pandas as pd
import nlpActions
import dns
import streamlit as st

from pymongo import MongoClient
from pprint import pprint


# Get creditials 
db_username = "do_not_checkin_secrets"
db_pass = "do_not_checkin_secrets"
db_clustername = "lp-do_not_checkin_secrets"

creds_str = "mongodb+srv://" + db_username + ":" +  db_pass +"@"+ db_clustername + ".zqqan.mongodb.net/<demo>?retryWrites=true&w=majority"
mongo_client = MongoClient(creds_str)
articles_db = mongo_client.demo
articles_collection = articles_db["article_analysis"]


omit_text = {"_id":0, "title":0, "body":0 }

omitted_for_lda_run = {"_id":0,
                         "title":0,
                         "publishDate":0,
                         "entityMentions":0,
                         "person":0,
                         "norp":0,
                         "fac":0,
                         "org":0,
                         "pge":0,
                         "loc":0,
                         "product":0,
                         "event":0,
                         "law": 0}

def get_sources():
    return articles_collection.distinct("sourceStandardized")


def get_articles_no_text():
    query = {}
    omitted_fields = omit_text
    return perform_query(query, omitted_fields)


def get_articles_by_sources_no_text(sources):
    in_dict = {"$in": sources}
    query = {"sourceStandardized": in_dict}
    return perform_query(query)


def get_articles_by_source(source):
    query = {"sourceStandardized": source}
    return perform_query(query)


def get_article_by_source_no_text(source):
    query = {"sourceStandardized": source}
    omitted_fields = omit_text
    return perform_query(query, omitted_fields)


def get_articles_by_publish_year_no_text(pub_year):
    query = {"publishYear": pub_year}
    omitted_fields = omit_text
    return perform_query(query, omitted_fields)


def get_articles_by_publish_year_month_no_text(pub_year, pub_month):
    query = {"publishYear": pub_year, "publishMonth": pub_month}
    omitted_fields = omit_text
    return perform_query(query, omitted_fields)


def get_articles_by_publish_year_month_range_no_text(start_pub_year, 
                                                 start_pub_month,
                                                 end_pub_year, 
                                                 end_pub_month):
    query = {
            "publishYear": {
                            "$gte": start_pub_year, 
                            "$lte": end_pub_year
                            },
            "publishMonth": {
                            "$gte": start_pub_month, 
                            "$lte": end_pub_month
                            }
            }
    omitted_fields = omit_text
    return perform_query(query, omitted_fields)


def get_article_by_id(id):
    query = {"id": id}
    return perform_query(query)


def perform_query(query, omitted_fields={}):
    if omitted_fields:
        cursor = articles_collection.find(query, omitted_fields)
    else:
         cursor = articles_collection.find(query, {"_id": 0})
    return list(cursor)


if __name__ == "__main__":
    art = get_article_by_id("a-80724908393")
    print(art)