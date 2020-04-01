# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class Ncaalinks_hfItem(scrapy.Item):
    # define the fields for your item here like:
    headers = scrapy.Field()
    page_links = scrapy.Field()
    table_html = scrapy.Field()
    url = scrapy.Field()
    stat_label = scrapy.Field()
    #week_no = scrapy.Field()
    last_updated = scrapy.Field()
    games_through = scrapy.Field()
    
    def index_fields(self):
        #be careful because the majority of these are of type selector
        return {
                #test object conversions in the pipeline class, but implement them here once proven
            'headers': self['headers'],
            'page_links': self['page_links'],
            'table_html': self['table_html'],
            'url': self['url'],
            'stat_label': self['stat_label'],
            'last_updated': self['last_updated'],
            'games_through':self['games_through']
         }
    def checkUnique(self):
        #self['table_html'] is of type SelectorList
        #verify whether the records are distinct
        for case in range(1,9,1):
            assert self['table_html'][case] != self['table_html'][case-1],"table rows are duplicated"
        return None
            
