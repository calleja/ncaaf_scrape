#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 29 22:07:03 2019

"""
import scrapy
from ncaalinks_19.items import Ncaalinks_hfItem
from datetime import datetime


class TeamstatsSpider(scrapy.Spider):
    name = "parsefollow"
    #the pages contained within this list will be downloaded
    start_urls = ['https://www.ncaa.com/stats/football/fbs']
    base_url = 'https://www.ncaa.com'
    
    def parse(self, response):
        links = response.xpath("//div/select[@id='select-container-team']/option/@value").getall()
        link_text = response.xpath("//div//select[@id='select-container-team']/option/text()").getall()
        #if we successfully retrieved some urls, write them out in case ncaa.com locks us out later allowing us to scrape the individual stat pages from urls saved to disk
        if len(links) > 0:
            print('we have found links')
            package = zip(link_text,links)
            #create a dict by iterating through two lists: 1) stat label, 2) url
            dic = {}
            for j in package:
                dic[j[0]]=j[1]
            #limit to three items during this testing phase
            dicy = list(dic.keys())[1:]
            #dicy_t = {key:dic[key] for key in dicy}
            #iterate through each url scraped from the drop down menu... the label is valuable here; I'd like to name the collection after this label
            '''TODO attempt to isolate the iteration to the problematic pages: 
            a) Blocked Punts Allowed
            b) Blocked Kicks Allowed
            c) Blocked Punts
            '''
            #TODO access only the problem tables:
            #dicy1 = ['Blocked Punts Allowed', 'Blocked Kicks Allowed', 'Blocked Punts']
            dicy_t = {key:dic[key] for key in dicy}
            #TODO create a dicy_t equivalent and pass below to continue testing...
            for label,link in dicy_t.items():
                    yield scrapy.Request(self.base_url + link, callback = self.parseAndExtractController, meta = {'page_name':label}, dont_filter=True)
                    #attempt to make use of the request... invoking "yield" should stop any further program flow within the function
                    #self.parseStatPage writes files; each of the files should be given a filename and stored individually

        elif len(links) == 0:
            print('no links and will revert to reading links from file')
            #case where I can no longer access the base_url; defer to the urls that have been stored successfully in the followlinks.txt file
        else:
            pass
            
    def parseAndExtractController(self, response):
        '''
        controller parsing class that is aware and keeps track of the secondary pages to parse 
        '''
        #headers should contain the label of the statistic (in a dict container)... eventually this will control the mongo collection to which this will be written
        label = response.meta
        #extract all the subsequent page links (NOTE: this will also extract the current page url)
        links = response.xpath('//ul[@class = "stats-pager"]//a/@href').getall()
        #remove duplicates (assumes links is a list of text)
        #remove duplicates (assumes links is a list of text)
        #assert type(links) == list, "links are not a list"
        #assert len(links) > 1, "link list contains 1 or less elements"
        if len(links) != 0:
            try:
                links_list = list(set(links))
                links_list.sort()
            #links_list.append(response.url)
                priority = 3
                print('contents of links_list within parseAndExtractController function (ensure that the parent page is included): {}'.format(links_list))
                for link in links_list:
                    print('sending url {} to the parseSingle spider in teamstats_items.py'.format(link))
                    priority -= 1
                    yield scrapy.Request(self.base_url + link, callback = self.parseSingle, meta = label, dont_filter=True, priority = priority)
            except (TypeError, ValueError, RuntimeError) as e:
                print("could not SET the links list properly; check object type. Errant code in teamstats_items.py")
        else:
            yield scrapy.Request(response.url, callback = self.parseSingle, meta = label, dont_filter=True)
        
    
    def parseSingle(self, response):
        '''
    a) discover headers and use freq to infer structure of the data table
    b) discover the page links for subsequent pages
    c) parse the main data table
    d) TODO make a call to the subsequent pages, ensuring not to unnecessarily call the original stats page
        '''
        def_item = Ncaalinks_hfItem()
        #response.xpath() returns a SelectorList instance - essentially a list of new selectors... appending with the getall() method returns text
        def_item['headers'] = response.xpath("//div[@class='stats-wrap']//th/text()").getall()
        #response.headers should contain the label of the statistic... eventually this will control the mongo collection to which this will be written... these headers are different than the above... this is the label, but is stored in the "headers" parameter
        def_item['stat_label'] = response.meta['page_name'].replace(' ','_')
        def_item['page_links'] = response.xpath('//ul[@class = "stats-pager"]//a/@href').getall()
        #this is a list that needs to be unraveled and converted to text... handle this after properly writing the other elements to file in a json format
        def_item['table_html'] = response.xpath("//div[@class='stats-wrap']//tr ")
        def_item['url'] = response.url
        #def_item['week_no'] = 1 ... not reliable at the moment
        #retrieve the dates of the system update
        date_list = []
        #for i in response.xpath("//div[@class='stats-header__lower__desc']/em[@class='placeholder']"):
            #date_list.append(i.text)
        date_list = response.xpath("//div[@class='stats-header__lower__desc']/em[@class='placeholder']/text()").getall()
            
        assert len(date_list) == 4, "date_list containing update timestamps doesn't contain 4 elems - teamstats_items.py; contents: {}".format(date_list)
        if len(date_list) == 4:
            def_item['last_updated'] = date_list[1]
            def_item['games_through'] = date_list[3] 
            
        #count number of elements successfully added to item "def_item"
        count = 0
        for k,v in def_item.index_fields().items():
            if v is not None:
                count += 1
        assert count == 7, "def_item doesn't contain standard 5 elements; check parseSingle"
        yield def_item
