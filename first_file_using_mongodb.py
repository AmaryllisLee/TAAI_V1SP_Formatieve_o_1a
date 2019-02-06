import pymongo
import pprint

from pymongo import MongoClient

client = MongoClient()
db = client.aai_database
collection = db['products']


# Wat is de naam en prijs van het eerste product in de database?
a = collection.find_one({},{'_id': '2583','name': 1,'price.selling_price': 1})
pprint.pprint(a)

# Geef de naam van het eerste product waarvan de naam begint met een 'R'?
b = collection.distinct('name',{'name':{'$regex':'^R'}})
pprint.pprint(b)
# Wat is de gemiddelde prijs van de producten in de database?
c = collection.aggregate(
    [
        {'$group':
             {'_id':'$_id','avgprice':
                 {'$avg':'$price.selling_price'}
              }
         }
    ]
)
for doc in c:
     pprint.pprint(doc)