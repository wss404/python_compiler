# Copyright 2009 Brian Quinlan. All Rights Reserved.
# Licensed to PSF under a Contributor Agreement.

"""Implements ThreadPoolExecutor."""

__author__ = 'Brian Quinlan (brian@sweetapp.com)'

import atexit
import itertools
import queue
import threading
import weakref
from concurrent.futures import _base
import os
import gc
import ctypes
import psutil

try:
    _malloc_trim = ctypes.CDLL('libc.so.6').malloc_trim
except:
    _malloc_trim = None


def memory_recycle(threshold_in_mb=512):
    if not _malloc_trim:
        return
    rss = psutil.Process(os.getpid()).memory_info().rss / 1024.0 / 1024.0
    if rss > threshold_in_mb:
        print('before memory_recycle, pid=%d, rss=%.1f M' % (os.getpid(), rss))
        gc.collect()
        _malloc_trim(0)
        print('after memory_recycle, pid=%d, rss=%.1f M' %
              (os.getpid(), psutil.Process(os.getpid()).memory_info().rss / 1024.0 / 1024.0))


# Workers are created as daemon threads. This is done to allow the interpreter
# to exit when there are still idle threads in a ThreadPoolExecutor's thread
# pool (i.e. shutdown() was not called). However, allowing workers to die with
# the interpreter has two undesirable properties:
#   - The workers would still be running during interpreter shutdown,
#     meaning that they would fail in unpredictable ways.
#   - The workers could be killed while evaluating a work item, which could
#     be bad if the callable being evaluated has external side-effects e.g.
#     writing to a file.
#
# To work around this problem, an exit handler is installed which tells the
# workers to exit when their work queues are empty and then waits until the
# threads finish.

_threads_queues = weakref.WeakKeyDictionary()
_shutdown = False


def _python_exit():
    global _shutdown
    _shutdown = True
    items = list(_threads_queues.items())
    for t, q in items:
        q.put(None)
    for t, q in items:
        t.join()


atexit.register(_python_exit)


class _WorkItem(object):
    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if not self.future.set_running_or_notify_cancel():
            return

        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as exc:
            self.future.set_exception(exc)
            # Break a reference cycle with the exception 'exc'
            self = None
        else:
            self.future.set_result(result)


class ThreadPoolExecutor(_base.Executor):
    # Used to assign unique thread names when thread_name_prefix is not supplied.
    _counter = itertools.count().__next__

    def __init__(self, max_workers=None, thread_name_prefix='',
                 max_tasks_per_worker=100, memory_recycle_threshold_in_mb=512):
        """Initializes a new ThreadPoolExecutor instance.

        Args:
            max_workers: The maximum number of threads that can be used to
                execute the given calls.
            thread_name_prefix: An optional name prefix to give our threads.
        """
        if max_workers is None:
            # Use this number because ThreadPoolExecutor is often
            # used to overlap I/O instead of CPU work.
            max_workers = (os.cpu_count() or 1) * 5
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        self._max_workers = max_workers
        self._memory_recycle_threshold_in_mb = memory_recycle_threshold_in_mb
        self._work_queue = queue.Queue()
        self._threads = set()
        self._thread_busy = {}
        self._thread_jobs = {}
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        self._thread_name_prefix = (thread_name_prefix or
                                    ("ThreadPoolExecutor-%d" % self._counter()))

    def set_current_thread_busy(self, busy):
        thread = threading.current_thread()
        if busy:
            self._thread_busy[thread] = True
            jobs = self._thread_jobs.get(thread, 0)
            self._thread_jobs[thread] = jobs + 1
        else:
            self._thread_busy[thread] = False

    def _has_free_thread(self):
        for _, busy in self._thread_busy.items():
            if not busy:
                return True
        return False

    def _worker(self):
        try:
            while True:
                work_item = self._work_queue.get(block=True)
                if work_item is not None:
                    self.set_current_thread_busy(True)
                    work_item.run()
                    # Delete references to object. See issue16284
                    del work_item
                    # if pool.is_current_thread_retiring():
                    #     return  # 线程退出
                    memory_recycle(self._memory_recycle_threshold_in_mb)
                    self.set_current_thread_busy(False)
                    continue

                if _shutdown:
                    # Notice other workers
                    self._work_queue.put(None)
                    return
                del executor
        except BaseException:
            _base.LOGGER.critical('Exception in worker', exc_info=True)

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')

            f = _base.Future()
            w = _WorkItem(f, fn, args, kwargs)

            self._work_queue.put(w)
            self._adjust_thread_count()
            return f

    submit.__doc__ = _base.Executor.submit.__doc__

    def _recycle_threads(self):
        dead_threads = []
        for thread in self._threads:
            if not thread.is_alive():
                dead_threads.append(thread)
        for thread in dead_threads:
            self._threads.remove(thread)
            del self._thread_jobs[thread]
            del self._thread_busy[thread]
            del thread

    def _adjust_thread_count(self):

        self._recycle_threads()
        num_threads = len(self._threads)
        if num_threads < self._max_workers and not self._has_free_thread():
            thread_name = '%s_%d' % (self._thread_name_prefix or self,
                                     num_threads)
            t = threading.Thread(name=thread_name, target=self._worker)
            t.daemon = True
            t.start()
            self._threads.add(t)
            _threads_queues[t] = self._work_queue

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            self._work_queue.put(None)
        if wait:
            for t in self._threads:
                t.join()

    def is_free(self):
        self._recycle_threads()
        thread_free = all(not busy for busy in self._thread_busy.values())
        return self._work_queue.qsize() == 0 and thread_free

    shutdown.__doc__ = _base.Executor.shutdown.__doc__


if __name__ == '__main__':
    import time

    executor = ThreadPoolExecutor(max_workers=2, max_tasks_per_worker=200)
    for i in range(20000):
        executor.submit(print, i)

    while not executor.is_free():
        time.sleep(1)
