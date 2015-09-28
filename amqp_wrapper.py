from traceback import format_exc
import socket
import sys
import time

from amqp import Connection, AMQPError

class Discard(Exception):
    pass

class Message:
    def __init__(self, f, queue, tag, *args, **kwargs):
        assert callable(f), "f must be a callable object"
        self._f = f 
        self._q = queue
        self._tag = tag
        self._args = args
        self._kwargs = kwargs

        self._is_quit = False
        self._requeue_exceptions = tuple()
        self._discard_exceptions = tuple()
        self.exception_callback = self._default_exception_callback

        self._init_channel()
		
		def _init_channel(self):
        while not self._is_quit:
		    	  try:
		        	self._conn = Connection(*self._args, **self._kwargs)
		        except socket.error:
		        	time.sleep(1)
		        else:
		        	self._chan = self._conn.channel()
		        	self._chan.basic_qos(0, 1, True)
		        	self._chan.basic_consume(queue=self._q, no_ack=False, 
		                    callback=self._wrap_f, consumer_tag=self._tag)
		          break

    def close(self):
        try:
            self._chan.close()
        except:
            pass
        try:
            self._conn.close()
        except:
            pass


    def _wrap_f(self, message, *args , **kwargs):
        try:
            self._f(message, *args, **kwargs)
        except (Discard, ) + self._discard_exceptions:
            print "Discard : ", format_exc()
            self._chan.basic_reject(message.delivery_tag, requeue=False)
        except Exception:
            self.exception_callback(self._chan, message)
        else:
            self._chan.basic_ack(message.delivery_tag)


    def _default_exception_callback(self, channel, message):
        print >>sys.stderr, "\n", message.body
        print >>sys.stderr, format_exc()
        print "Requeue : ", format_exc()
        channel.basic_reject(message.delivery_tag, requeue=True)


    def loop(self):
        while not self._is_quit:
            try:
                self._conn.drain_events(timeout=3)
            except socket.timeout:
                pass
            except (AMQPError, IOError) as exc:
                if "[Errno 4] Interrupted system call" in str(exc):
                    print str(exc)
                    continue
                print "failed to communacate with rabbitmq, trying reconnect..."
                self._init_channel()
        self.close()

    def __del__(self):
        self.close()

    def is_quit(self, quit):
        self._is_quit = bool(quit)

    def set_requeue_exceptions(self, *args, **kwargs):
        self._requeue_exceptions = args

    def set_discard_exceptions(self, *args, **kwargs):
        self._discard_exceptions = args

    def set_exception_callback(self, f):
        assert callable(f), "f must be a callable object"
        self.exception_callback = f

