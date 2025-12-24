from pymongo import MongoClient

client = MongoClient('localhost',27017)

print("MongoDB databases:", client.list_database_names())

if 'Boutique' in client.list_database_names():
    print("'Boutique' database mawjouda")
    db = client['Boutique']
    print("Collections:", db.list_collection_names())

# IT WORK I TEST IT 
# adhaya chytl3lk ki truni l programme
#MongoDB databases: ['Boutique', 'admin', 'config', 'local']
#'Boutique' database mawjouda
#Collections: ['products', 'orders', 'customers', 'predictions'] 