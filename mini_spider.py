#!/usr/bin/env python
# -*- coding: utf-8 -*-

######################################################################
#
# Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved.
#
######################################################################

"""
Python codemaster: a implementation of mini-spider.

Authors: zhangzhibiao01(zhangzhibiao01@baidu.com)
Date:        2014/02/18 14:24:30
"""

import BeautifulSoup
import chardet
import ConfigParser
import getopt
import logging
import os
import Queue
import re
import sys
import threading
import time
import urllib2
import urlparse

import threadPool
#import stacktracer

__version = 1.0

def usage():
    """
    Usage
    """
    print 'python mini_spider.py -c spider.conf [-d]\nversion : %s' % __version


class Node(object):
    """
    Queue node, include url and depth
    """
    def __init__(self, url="", depth=0):
        self.url = url
        self.depth = depth


class Spider(object):
    """
    Mini spider implementation.
    """
    def __init__(self, conf, logname='log'):
        """
        Constructor.
        @conf, config
        @logname, name of log, log default.
        """
        self.conf = conf
        self.log = logname
        self.thread_num = 0
        self.output = ""
        self.max_depth = 1
        self.interval = 1 # 1s
        self.timeout = 1 # 1s
        self.target_url = ""
        self.target_url_reg = ""
        self.crawled_sets = set()
        self.urlQueue = Queue.Queue()
        self.event = threading.Event()
        self.rlock = threading.RLock()
        # load log module
        logging.basicConfig(
                filename=os.path.join(os.getcwd(), self.log),
                level=logging.DEBUG,
                format = '%(asctime)s - %(levelname)s: %(message)s'
                )

    def readConf(self):
        """
        Read config.
        Return value, True for success, False for fail.
        """
        # read conf
        try:
            cf = ConfigParser.ConfigParser()
            cf.read(self.conf)
            self.thread_num = int(cf.get('spider', 'thread_count'))
            self.output = cf.get('spider', 'output_directory')
            self.max_depth = int(cf.get('spider', 'max_depth'))
            self.interval = int(cf.get('spider', 'crawl_interval'))
            self.timeout = int(cf.get('spider', 'crawl_timeout'))
            self.target_url = cf.get('spider', 'target_url')
            for line in open(cf.get('spider', 'url_list_file')):
                if line.startswith('http'):
                    self.urlQueue.put(Node(url=line.strip(' /\n\r')))
            self.event.set()
        except:
            logging.warning('read conf [%s] failed!', self.conf)
            return False

        # mkdir for output && log
        outputdir = os.path.join(os.getcwd(), self.output)
        try:
            if not os.path.exists(outputdir):
                os.mkdir(outputdir)
        except os.error as e:
            logging.warning('mkdir failed %s', outputdir)
            return False
        self.output = outputdir
        self.target_url_reg = re.compile(self.target_url)
        return True

    def save_if_need(self, url, content):
        """
        Check if need save content.
        """
        if self.target_url_reg.match(url):
            f = open(os.path.join(self.output, url.replace('/', '_')), 'w')
            f.write('%s' % content)
            f.close()
    
    def parse_links(self, base_url, content):
        """
        Parse hyperlink from html page.
        """
        alllinks = []
        links = BeautifulSoup.BeautifulSoup(content).findAll('a', href=re.compile('^http|^/|^\.'))
        base_url = base_url.strip('/ ')
        for item in links:
            if not item['href'].startswith('http'):
                alllinks.append(urlparse.urljoin(base_url, item['href']).strip('/ '))
            else:
                alllinks.append(item['href'].strip('/ '))
        #logging.debug('%s', alllinks)
        return alllinks

    def crawl_work(self, node):
        """
        Crawler main work function.
        """
        try:
            #print node.url
            logging.debug("url start[%s]", node.url)
            r = urllib2.urlopen(node.url, timeout = self.timeout)
            if r.getcode() != 200:
                logging.warning('Crawl [%s] failed, ret code[%d]', node.url, r.getcode())
                time.sleep(self.interval)
                return None
            content = r.read()
            logging.info('Crwaled url[%s], depth[%d], len [%d]', node.url, node.depth, len(content))
            # save if necessay
            self.save_if_need(node.url, content)
            # parse more links
            self.rlock.acquire()
            for link in self.parse_links(node.url, content):
                # check node depth and not crawled
                if node.depth < self.max_depth and link not in self.crawled_sets:
                    self.urlQueue.put(Node(link, node.depth+1))
            self.rlock.release()
            logging.debug("url done[%s]", node.url)
            if not self.event.isSet():
                self.event.set()
        except:
            #raise
            return None
        return True

    def start(self):
        """
        Object start function.
        """
        # read config
        if not self.readConf():
            return None
        # thread pool
        self.threadpool = threadPool.ThreadPool(self.thread_num, self.timeout)
        self.threadpool.start_threads()
        # logging start 
        logging.info('Begin crawling...')
        # url队列退出控制变量
        emptyCount = 0
        maxEmptyCount = 60
        # 主线程从url队列中获取url,并放到工作队列中
        # main thread get url node from urlQueue, and 
        # put it in threadpool
        while True:
            if self.urlQueue.empty():
                if not self.event.wait(self.timeout + 2):
                    break
            self.event.clear()
            emptyCount = 0
            try:
                item = self.urlQueue.get(timeout=self.timeout + 2)
            except Queue.Empty:
                logging.debug("urlQueue empty")
                continue
            self.threadpool.add_job(self.crawl_work, item)
            self.rlock.acquire()
            self.crawled_sets.add(item.url)
            self.rlock.release()
            self.urlQueue.task_done()
        self.stop()
        logging.info('Done')

    def stop(self):
        """
        Stop all.
        """
        self.threadpool.stop_threads()
        self.urlQueue.join()

    def test(self):
        """
        Self simple test function.
        """
        url = 'http://www.baidu.com'
        links = self.parse_links(url, urllib2.urlopen(url).read())
        for i in links:
            print i
        print len(links)


def main():
    """
    Main function.
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:vt")
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)
    conf_file = ""

    verbose = False
    test_flag = False
    for o, a in opts:
        if o == "-v":
            print version
        elif o == "-h":
            usage()
            sys.exit()
        elif o == "-c":
            conf_file = a
        elif o == '-t':
            test_flag = True
        else:
            assert False, "unhandled option"

    if len(conf_file) <= 0:
        usage()
        sys.exit(2)
    #stacktracer.trace_start("trace.html",interval=1,auto=True)
    spider = Spider(conf_file)
    if test_flag:
        spider.test()
    else:
        spider.start()
    #stacktracer.trace_stop()

if __name__ == '__main__':
    main()
