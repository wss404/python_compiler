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
