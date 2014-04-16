#!/usr/bin/env python
#--encoding: utf8--

######################################################################
#
# Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved.
#
######################################################################

"""
Test for a implementation of mini-spider.

Authors: zhangzhibiao01(zhangzhibiao01@baidu.com)
Date:        2014/02/19 18:24:25
"""

import os
import unittest
from mini_spider import Spider


class TestMiniSpider(unittest.TestCase):
    '''
    Unittest class for mini-spider.
    '''
    def setUp(self):
        '''
        SetUp method. 
        '''
        self.spider = Spider("./spider.conf")

    def tearDown(self):
        pass
    
    def testConf(self):
        '''
        Test load conf
        '''
        self.assertTrue(self.spider.readConf())

    def test_save_if_need(self):
        '''
        Test function save_if_need.
        '''
        self.spider.readConf()
        url = "http://www.baidu.com/1.html"
        self.spider.save_if_need(url, "test")
        self.assertTrue(os.path.exists(self.spider.output + '/' + url.replace('/', '_')))

    def test_parse_links(self):
        '''
        Test parse_links.
        '''
        url = "http://www.baidu.com"
        page = '<a href="http://www.w3school.com.cn/">Visit W3School</a>'
        res = self.spider.parse_links(url, page)
        self.assertTrue(res[0] == "http://www.w3school.com.cn")

if __name__ == '__main__':
    unittest.main()
