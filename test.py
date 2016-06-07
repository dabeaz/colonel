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
    t1 = await spawn(counter('Count1', '10'))
    print('Task --->', t1)
    result = await t1.join()
    print('Result --->', result)
    print('Done')

colonel.run(test2('Dave'))


