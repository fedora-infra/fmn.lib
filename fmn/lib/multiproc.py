import logging
import sys
import threading
import traceback as tb

from multiprocessing import Queue, Process

__all__ = ['FixedPool']

log = logging.getLogger('fedmsg')


class FixedPool(object):
    """ Our own multiprocessing pool.

    This avoids the 'importable' requirement of multiprocessing.Pool.
    """
    def __init__(self, N):
        log.info('Initializing fmn multiproc pool, size %i for thread %s' % (
            N, threading.current_thread().name))
        self.incoming = Queue()
        self.outgoing = Queue()
        self.processes = []
        self.N = N

    @property
    def targeted(self):
        # If I have processes, then I am targetted on some func.
        return bool(self.processes)

    def target(self, fn):
        log.info('Multiprocessing pool targeting %r' % fn)
        if self.targeted:
            self.close()

        args = (fn, self.incoming, self.outgoing)
        self.processes = [
            Process(target=work, args=args) for i in range(self.N)]

        for p in self.processes:
            p.daemon = True
            p.start()

        log.info('Multiprocessing pool targeting done.')

    def apply(self, items):
        """ Items are not guaranteed to come back in the same order. """
        if not self.targeted:
            raise ValueError('.target(fn) must be called before .apply(items)')
        for item in items:
            if not isinstance(item, tuple):
                item = item,
            self.incoming.put(item)
        results = [self.outgoing.get() for i in range(len(items))]
        for result in results:
            if isinstance(result, Exception):
                raise result
        return results

    def close(self):
        if not self.targeted:
            log.warning('No need to close pool.  Not yet targeted.')
            return

        log.info('Closing fmn multiproc pool.')
        for _ in self.processes:
            self.incoming.put(StopIteration)

        # XXX - we could call process.join() on all of our child processes, but
        # that only works if *this* process is the same PID as the process that
        # created them, and that isn't always the case when under the care of
        # Twisted.  So, we'll just omit that.  Twisted cleans itself up nicely
        # without it.

        self.processes = []
        log.info('Multiproc pool closed.')

def work(fn, incoming, outgoing):
    while True:
        args = incoming.get()
        if args is StopIteration:
            break
        try:
            result = fn(*args)
        except Exception as e:
            result = type(e)(
                "... which was originally caused by:\n" +
                "".join(tb.format_exception(*sys.exc_info())))
        finally:
            outgoing.put(result)
