import colonel
from types import coroutine

@coroutine
def get_kernel():
    yield (11, )

@coroutine
def get_current():
    return (yield (12, ))

@coroutine
def spawn(coro, daemon=False):
    return (yield (3, coro, daemon))

@coroutine
def join(task):
    return (yield (5, task))

@coroutine
def sleep(seconds):
    yield (2, seconds)

async def counter(label, n):
    total = 0
    while n > 0:
        print(label, n)
        await get_current()
        total += n
        n -= 1
    return total

async def test0(name):
    print('Hello', name)
    self = await get_current()
    print('Task -->', self)
    print('Done')

async def test1(name):
    print('Hello', name)
    t1 = await spawn(counter('Count1', 10))
    print('Task --->', t1)
    t2 = await spawn(counter('Count2', 10))
    print('Task --->', t2)
    print('Done')

async def test2(name):
    print('Hello', name)
    t1 = await spawn(counter('Count1', 10))
    print('Task --->', t1)
    result = await t1.join()
    print('Result --->', result)
    print('Done')

async def test3():
    print('About to sleep')
    await sleep(5.00)
    print('Back from sleeping')
    print('Sleeping again')
    await sleep(3)
    print('Back')

from curio.errors import CancelledError

async def spinner(label, n, interval):
    try:
        while n > 0:
            print(label, n)
            await sleep(interval)
            n -= 1
    except CancelledError:
        print('Cancelled')

async def test4():
     await spawn(spinner('spin1', 10, 1))
     await spawn(spinner('spin2', 30, 0.25))
     await spawn(spinner('spin3', 5, 2))

async def test5():
     t = await spawn(spinner('spin1', 10, 1))
     await sleep(5)
     await t.cancel()
     print('Test5 done')

async def consumer(q):
     print("Consumer")
     while True:
          item = await q.get()
          if item is None:
              break
          print('Got:', item)

async def producer(q):
     print("Producer")
     for i in range(10):
         await q.put(i)
         await sleep(1)
     await q.put(None)

from curio.queue import Queue

async def test6():
     q = Queue()
     await spawn(consumer(q))
     await spawn(producer(q))


from curio import timeout_after, TaskTimeout

async def test7():
    try:
        await timeout_after(5, sleep(10))
    except TaskTimeout:
        print("Timeout!")


import time
def add(x,y):
    print('Adding %s %s' % (x, y))
#    time.sleep(5)
    return x + y

from curio import run_in_thread

async def test8():
    for n in range(100):
        r = await run_in_thread(add, n, n)
        print("Got:", r)

from curio import SignalSet
import signal
import os

async def test9():
    import os
    print("PID", os.getpid())
    while True:
        async with SignalSet(signal.SIGUSR1, signal.SIGUSR2) as sig:
            signo = await sig.wait()
            print("Got signal", signo)

colonel.run(test8())


