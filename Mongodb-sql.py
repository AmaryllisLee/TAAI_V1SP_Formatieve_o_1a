import pymongo
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pymongo import MongoClient
import pprint

client = MongoClient()
db = client.aai_database
collection = db['products']
#----function to create db if exist ------
def create_database(a):
       'create database if it doesn\'t exist'
       con = psycopg2.connect(dbname='postgres', user='postgres', password='amaryllis')
       con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
       cur = con.cursor()
       if cur.execute('SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower(\'{}\')'.format(a)) == False:
        cur.execute('CREATE DATABASE \'{}\')'.format(a))
       else:
           pass
       cur.close()
       con.close()


#function to create table if exist------
def create_table(a):
    'create table in db if table doesn\t exist'
    con = psycopg2.connect(dbname=a, user='postgres', password='amaryllis')
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    if cur.execute('select exists (select * from information_schema.tables where table_name =\'products\')') == True:
        cur.execute('CREATE TABLE Products('
                    'product_id int,'
                    'name varchar(255) ,'
                    'price decimal(10,2))')  # table products

    else:
       pass
    cur.close()
    con.close()
#-------------------------------retrieve the name, price and id from Mongo db---------------------------------------------
#STep 1: rertrieve necessary field from collection product in Mongodb
def get_data():
    'retrieve data from Mongodb'
    lst= []
    for x in collection.find({},{'name':1,'price.selling_price': 1}):
        lst.append(x)
    return(lst)
#----------------------------------------------------------------------------

a= get_data()
c= 'db_f_opdracht'
db= create_database(c)
create_table(c)

con = psycopg2.connect(dbname=c,user= 'postgres',password='amaryllis')
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor()
for x in a:
    cur.execute('INSERT into Products(product_id,name, price) VALUES(\'{},{},{}\')'.format(x['_id'],['name'],['price']['selling_price']))
con.commit()

con = psycopg2.connect(dbname=c,user= 'postgres',password='amaryllis')
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor()
cur.execute('select Avg(prijs) from products ')
con.commit()

con = psycopg2.connect(dbname=c,user= 'postgres',password='amaryllis')
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor()
cur.execute('select ABSOLUTE (prijs) from products ')
con.commit()





