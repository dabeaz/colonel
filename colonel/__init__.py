# colonel/__init__.py

from . import _colonel
from curio.traps import _join_task, _cancel_task
from curio.errors import TaskError
from curio.kernel import Kernel
import socket

class Task(_colonel.CTask):
    def __repr__(self):
        return 'Task(id=%r, %r, state=%r)' % (self.id, self.coro, self.state)

    def __str__(self):
        return self.coro.__qualname__

    async def join(self):
        await _join_task(self)
        if self.next_exc:
            raise TaskError('Task crash') from self.next_exc
        else:
            return self.next_value

    async def cancel(self):
        print("CANCEL HERE1")
        if self.terminated:
            return False
        else:
            await _cancel_task(self)
            return True

class Colonel(Kernel):
    # Helper method to attach a callback to a future (easier done in Python than in C)
    def _add_future_callback(self, future, task):
        future.add_done_callback(lambda fut: self._wake(task, future))

    # Helper method to assist with signal handling (easier in Python than in C)
    def _signal_helper(self, signo):
        if signo not in self._signal_sets:
            return []

        tasks = []
        for sigset in self._signal_sets[signo]:
            sigset.pending.append(signo)
            if sigset.waiting:
                tasks.append(sigset.waiting)
                sigset.waiting = None
        return tasks

    def run(self, coro=None, *, shutdown=False):
        if coro:
            result = _colonel.run(self, coro, Task)
        else:
            result = None
        if shutdown:
            self._shutdown_resources()
        return result

def run(coro):
    kernel = Colonel()
    return kernel.run(coro, shutdown=True)
