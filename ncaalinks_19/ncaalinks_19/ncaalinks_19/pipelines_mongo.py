# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import pymongo

class NcaalinksPipeline(object):
    def __init__(self, mongo_uri, mongo_db): 
        '''
        connection = pymongo.MongoClient(
        settings[‘MONGODB_SERVER’],
        settings[‘MONGODB_PORT’]
)
        #static database selection
        self.db = connection[settings[‘MONGODB_DB’]]

'''        
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            #'items' is the default argument to the get()
            mongo_db=crawler.settings.get('MONGO_DB', 'items')
        )
    #scrapy.craler provides access to all Scrapy core components

    def open_spider(self, spider):
        #can opt to establish the connection to db
        #simply mentioning the spider here is good enough
        #have all spiders call eachother from within the spider class
        
        #check out mongoclient API: https://api.mongodb.com/python/current/api/pymongo/mongo_client.html
        self.client = pymongo.MongoClient(self.mongo_uri) 
        self.db = self.client[self.mongo_db]
        #check whether a reliable connection is established
        try:
            self.client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError as err:
            print('failed to connect to db, error message: {}'.format(err))
    
    def close_spider(self, spider):
        #can opt to close the db connection
        #working w/db connections in this manner is efficient, because these are resources that are best enabled only once during the routine
        self.client.close()
    
    def process_item(self, item, spider):
        #Interaction with the item derived from the stats pages
        #must either: return a dict with data, return an Item (or any descendant class) object, return a Twisted Deferred or raise DropItem exception
        #can opt to insert the data into the db (opened in the open_spider) function
        #in order to verify that everything is working, should write output to a file
        item_dic = item.index_fields()
        #verify that item_dic['stat_label'] works as expected; this value will serve as the collection name for mongoDB... the words will need to be formatted to remove the spacing (trimmed) in order for it to be serviceable for a collections name in mongo
        
        print('stat_label is {}'.format(item_dic['stat_label']))
        
        assert type(item_dic['stat_label']) == str, "stat_label field within the item is not a string"
        
        filename = item_dic['url']  
        topline = '/home/merde/Documents/scrapy_projects/ncaalinks_19/programOutput/'
        #run the item method that verifies uniqueness on each table row
        checkunique = item.checkUnique()
        #PURPOSE: download the html table data of each stat and its child pages (ex. pg2, pg3)
        #split the filename on the backslashes and use the last element as the file title: ex. pageteam_699.json
        try:
            file_list = filename.split('/')
            assert len(file_list[-2:]) == 2, "file list is not equal to two"
            realname = '_'.join(file_list[-2:])
            
            #parse the html table and store in container for json file writing
            def parse_table(item):
                #unpack the requisite elements from the element
                #headers should be a list of text
                columns_no = len(item['headers'])
                standard = 'td[{}]/text() | td[{}]/a/text()'
                #dict to hold the xpath query syntax to be applied to the elements of the SelectorList... each element of the SelectorList is html of the the data table row. NOTE: stat categories are not printed to ea row; we reference the item['headers'] element for that 
                new_dict = {}
                #this is a list containing text
                headers = item['headers']
                #assert the class type
                assert type(headers) == list, "headers is of type {}".format(type(headers))
                #building the xpath expression: an iterative process to build only the RAW xpath script to be applied to the html_table. Dict format is dict[stat_category]: table data from row of html table
                for i in range(columns_no):
                    #may not be retrieving text when indexing the headers element because it may be stored as a selector
                    #this sequenced approach is possible bc we assume that elements are stored in order - headers-to-data elements (in the html table)
                    new_dict[headers[i]] = standard.format(i+1,i+1)
                lista= [] # a list of dictionaries
                
                #item['table_html'] is of type SelectorList; this loop/program below assumes a SelectorList with elements of multiple... skipping the first element bc that's the header row
                #print_list = [i.getall() for i in item['table_html']]
                #print('here is the first html table: {}'.format(print_list[0:6]))
                true_rank = 0
                for row in item['table_html'][1:]:
                    #retrieve all the text contained within the element... getall() returns a serialized html to text
                    #getAllList = row.getall() <- test code
                    #mesa_row is a dict that will store all the stats printed on each row of the html table... later, this dict will be appended to a list ("lista")
                    #mesa_row k,v ex: mesa_row['total_defense']: 6
                    mesa_row = {}
                    
                    #insert the date and week number from the high-level item; these values are controlled in teamstats_items.py
                    #mesa_row['week_no'] = item['week_no']
                    #mesa_row['week_ended_date'] = item['week_ended_date']
                    mesa_row['games_through'] = item['games_through']
                    mesa_row['last_updated'] = item['last_updated']
                    for k,v in new_dict.items():
                        #row.xpath(v) below is NOT text. The xpath specifies the index within the html data table row that contains the element corresponding with a particular header
                        one_row_extract = row.xpath(v).get()
                        #the above appears to errantly duplicating the dictionary
                        #assert len(one_row_extract) ==2, "the xpath extraction yielded something other than two"
                        #the row extract, as asserted here, is not accurate: the xpath extracted element-wise data
                        #print("this is the dict extract: {}".format(one_row_extract))
                        #this doesn't cause a duplicate
                        mesa_row[k] = one_row_extract
                    #will need to be aware of the rank, because will need to populate missing ranks (with an increment)
                    #print('these are the indexes to the html table: {}'.format(mesa_row.keys()))
                    try:
                        rank = mesa_row['Rank']
                    except KeyError as e:
                        rank = mesa_row['RANK']
                    assert type(rank) is str, "check pipelines_mongo... rank is not str type"
                    try:
                        true_rank = int(rank)
                    except ValueError:
                        true_rank = true_rank+1
                        mesa_row['Rank'] = str(true_rank)
                    
                    lista.append(mesa_row)
                    
                    #previously a list of the text serialized content of each selector element; now, we'll try to extract the table data
                return(lista)
                
            try:
                #results are in the form of a list of dictionaries
                lista_f = parse_table(item_dic)
                
                #may want to iterate through the list to insert into the mongodb... trying bulk insertion first
                self.db[item_dic['stat_label']].insert_many(lista_f)
                #place this list of dictionaries into the mongodb collection having the name of the stats class label
                
                
                
            except (RuntimeError, TypeError, NameError) as e:
                print('table was not parsed due to error')
                print(e)
            
        except AttributeError as e:
            print("topmost try in the divertPipeline caught an error")
            print(e)
        #write the response to file
        result_dic = {item['url']:'completed'}
        return result_dic
