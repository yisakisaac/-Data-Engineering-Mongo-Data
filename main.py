import pymongo
import pandas as pd

def main():
    '''
    connect to mongoDB atlas using given credentials and upload users.csv and new_user.csv to mongoDB
    into a single collection. Output 4 csv files containing filtered and sorted data.
    '''
    
    client = pymongo.MongoClient("mongodb://YIsaac:4qGzY3rJG9JpIg63@bigdatatest-shard-00-00.mv5js.mongodb.net:27017,bigdatatest-shard-00-01.mv5js.mongodb.net:27017,bigdatatest-shard-00-02.mv5js.mongodb.net:27017/?ssl=true&replicaSet=atlas-nbvcs0-shard-0&authSource=admin&retryWrites=true&w=majority")
    
    users_to_mongo()
    new_users_to_mongo()
    responded_in_first_half_of_2020()
    responded_unfavorably_in_sept_2020()
    not_tried_to_contact_in_2020()
    missing_first_name()
    

def users_to_mongo():
    '''
    upload users.csv to mongoDB users collection
    '''
    
    client = pymongo.MongoClient("mongodb://YIsaac:4qGzY3rJG9JpIg63@bigdatatest-shard-00-00.mv5js.mongodb.net:27017,bigdatatest-shard-00-01.mv5js.mongodb.net:27017,bigdatatest-shard-00-02.mv5js.mongodb.net:27017/?ssl=true&replicaSet=atlas-nbvcs0-shard-0&authSource=admin&retryWrites=true&w=majority")
    mg_db = client.AlRahrooh
    mg_users = mg_db.users
    users_df = pd.read_csv("users.csv")
    users_data = users_df.to_dict(orient = 'records')
    mg_users.insert_many(users_data)


def new_users_to_mongo():
    '''
    upload new_users.csv to mongoDB users collection
    '''
    
    client = pymongo.MongoClient("mongodb://YIsaac:4qGzY3rJG9JpIg63@bigdatatest-shard-00-00.mv5js.mongodb.net:27017,bigdatatest-shard-00-01.mv5js.mongodb.net:27017,bigdatatest-shard-00-02.mv5js.mongodb.net:27017/?ssl=true&replicaSet=atlas-nbvcs0-shard-0&authSource=admin&retryWrites=true&w=majority")
    mg_db = client.AlRahrooh
    mg_users = mg_db.users
    new_users_df = pd.read_csv("new_user.csv")
    new_users_data = new_users_df.to_dict(orient = 'records')
    mg_users.insert_many(new_users_data)


def collect_data():
    '''
    establishes connection to mongoDB client and downloads users collection as a local dataframe
    '''

    client = pymongo.MongoClient("mongodb://YIsaac:4qGzY3rJG9JpIg63@bigdatatest-shard-00-00.mv5js.mongodb.net:27017,bigdatatest-shard-00-01.mv5js.mongodb.net:27017,bigdatatest-shard-00-02.mv5js.mongodb.net:27017/?ssl=true&replicaSet=atlas-nbvcs0-shard-0&authSource=admin&retryWrites=true&w=majority")
    mg_db = client.AlRahrooh
    mg_users = mg_db.users
    all_records = mg_users.find()
    list_cursor = list(all_records)
    global df
    df = pd.DataFrame(list_cursor)


def responded_in_first_half_of_2020():
    '''
    creates csv containing those that have responded in the first half of 2020 and is sorted by response date
    '''

    collect_data()
    filtered = df[(df['LASTREPLY'] > '2020-01-01') & (df['LASTREPLY'] < '2020-06-31')]
    sorted = filtered.sort_values('LASTREPLY')
    sorted.to_csv("output/responded_in_first_half_of_2020.csv")


def responded_unfavorably_in_sept_2020():
    '''
    creates csv containing those that have responded unfavorably in September of 2020 and is sorted by response date
    '''
    
    collect_data()
    df['LASTSENDDATE'] = pd.to_datetime(df['LASTSENDDATE'])
    unfavorable_responses = ['UNSURE', 'DONOTCONTACT', 'NEGATIVE']
    filtered = df[df['RESPONSECATEGORY'].isin(unfavorable_responses)]
    filtered_2 = filtered[(filtered['LASTREPLY'] > '2020-09-01') & (filtered['LASTREPLY'] < '2020-09-31')]
    sorted = filtered_2.sort_values('LASTREPLY')
    sorted.to_csv("output/responded_unfavorably_in_sept_2020.csv") 


def not_tried_to_contact_in_2020():
    '''
    creates csv containing those that have not tried to be contacted in 2020 and sorts by send date
    '''
    
    collect_data()
    df['LASTSENDDATE'] = pd.to_datetime(df['LASTSENDDATE'])
    sorted = df.sort_values('LASTSENDDATE')
    filtered = sorted[(sorted['LASTSENDDATE'] >= '2021-01-01')]
    filtered_2 = sorted[(sorted['LASTSENDDATE'] <= '2019-12-31')]
    result = pd.concat([filtered_2, filtered])
    result.to_csv("output/not_tried_to_contact_in_2020.csv") 


def missing_first_name():
    '''
    creates csv containing those that have missing First Name and is sorted by last name
    '''
    
    collect_data()
    filtered = df[df['FIRST'].isnull()]
    sorted = filtered.sort_values('LAST')
    sorted.to_csv("output/missing_first_name.csv") 


main()

