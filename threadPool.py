# -*- coding: utf-8 -*-

######################################################################
#
# Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved.
#
######################################################################

"""
A thread-pool with message queue implemented using threading.

Users only provide a work routine.
Usage:
    import threadPool
    tp = threadPool.ThreadPool(threadNum, timeOut)
    tp.start()
    tp.addJob(work_func, args)
    tp.stop()

Authors: zhangzhibiao01(zhangzhibiao01@baidu.com)
Date: 2013/02/18 17:24:26
"""

import Queue
import threading

class Worker(threading.Thread):
    """
    Worker, which does the real job.
    """
    def __init__(self, threadPool, *args, **kargs):
        threading.Thread.__init__(self)
        self.threadPool = threadPool
        self.setDaemon(True)
        self.state = None    #线程工作状态
        self.start()

    def run(self):
        """
        Running routine.
        """
        while True:
            if self.state == 'STOP':
                break
            try:
                func, args, kargs = self.threadPool.workQueue.get(self.threadPool.timeOut)
            except Queue.Empty:
                continue
            try:
                res = func(*args, **kargs)
                self.threadPool.resultQueue.put(res)
                self.threadPool.workDone()
            except:
                break

    def stop(self):
        """
        Stop workers.
        """
        self.state = 'STOP'
            

class ThreadPool(object):
    """
    Thread pool module.
    """
    def __init__(self, threadNum, timeOut):
        """
        Constructor.
        @threadNum, num of threads
        @timeOut, time waiting on queue in seconds.
        """
        #工作队列
        self.workQueue = Queue.Queue()
        #结果队列
        self.resultQueue = Queue.Queue()
        #线程池
        self.threadPool = []
        #线程数目
        self.threadNum = threadNum
        #队列读等待超时时间, 
        #写无须设置超时时间, 
        #因为队列无长度限制
        self.timeOut = timeOut

    def start_threads(self):
        """
        Start threads.
        """
        for i in range(self.threadNum):
            self.threadPool.append(Worker(self))

    def work_join(self, *args, **kargs):
        """
        Join the workers.
        """
        self.workQueue.join()

    def add_job(self, func, *args, **kargs):
        """
        Add an job to pool.
        """
        self.workQueue.put((func, args, kargs))

    def work_done(self, *args):
        """
        Called when job done.
        """
        self.workQueue.task_done()

    def get_result(self, *args, **kargs):
        """
        Get result from result queue
        """
        return self.resultQueue.get(*args, **kargs)

    def stop_threads(self):
        """
        Stop all threads.
        """
        for thread in self.threadPool:
            #thread.join()
            thread.stop()
        del self.threadPool[:]
