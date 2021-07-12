from pymongo import MongoClient
"""
db->collections->documents: 
db->tables->json style / dictionaries

"""
def get_db_handle(db_name, host, port, username, password):

 client = MongoClient(host=host,
                      port=int(port),
                      username=username,
                      password=password
                     )
 db_handle = client['db_name']
 return db_handle, client

