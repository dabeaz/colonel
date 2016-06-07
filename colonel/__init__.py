# colonel/__init__.py

from . import _colonel
from curio.traps import _join_task
from curio.errors import TaskError

class Task(_colonel.CTask):
    def __repr__(self):
        return 'Task(id=%r, %r, state=%r)' % (self.id, self.coro, self.state)

    def __str__(self):
        return self.coro.__qualname__

    async def join(self):
        print("JOINING!")
        await _join_task(self)
        print("BACK", type(self.next_exc), repr(self.next_exc))
        if self.next_exc:
            raise TaskError('Task crash') from self.next_exc
        else:
            return self.next_value

def run(coro):
    return _colonel.run(coro, Task)
