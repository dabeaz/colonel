/* colonel.c

   Pure C implementation of the Curio kernel.  Highly experimental.
*/

#include "Python.h"
#include "structmember.h"
#include <unistd.h>
#include <sys/socket.h>
#include <assert.h>

/* Exceptions */

PyObject *Curio_CancelledError;
PyObject *Curio_CancelRetry;
PyObject *Curio_TaskTimeout;
PyObject *Curio_TaskError;

PyObject *APPEND_METHOD;
PyObject *REMOVE_METHOD;
PyObject *POPLEFT_METHOD;

/* Timeout Management */

#define TIMEOUT_SLEEP 1
#define TIMEOUT_TIMEOUT 2

typedef struct Timeout {
  _PyTime_t         timeout;
  int               type;
  struct CTask      *task;
  struct Timeout    *next;
} Timeout;

Timeout *new_Timeout(_PyTime_t timeout, struct CTask *task, int type) {
  Timeout *t = (Timeout *) malloc(sizeof(Timeout));
  t->timeout = timeout;
  t->task = task;
  t->type = type;
  t->next = NULL;
  return t;
}

void delete_Timeout(Timeout *t) {
  free(t);
}

/* Cancellation Support */

typedef struct Kernel Kernel;
typedef struct CTask CTask;

typedef void (*cancelfunc_t)(struct Kernel *, struct CTask *);

#define EVENT_READ 1
#define EVENT_WRITE 2

typedef struct Event {
  int op;
  int fileno;
  void *data;
} IOEvent;

#ifdef HAVE_KQUEUE
#ifdef HAVE_SYS_EVENT_H
#include <sys/event.h>
#endif

#define MAX_EVENT 64
typedef struct Selector {
  int kq;
} Selector;

Selector *
new_Selector(void) {
  Selector *sel;
  sel = (Selector *) malloc(sizeof(Selector));
  sel->kq = kqueue();
  return sel;
}

void
delete_Selector(Selector *sel) {
  close(sel->kq);
  free(sel);
}

void Selector_register(Selector *sel, int op, int fileno, void *data) {
  struct kevent event;
  int filter = 0;

  if (op & EVENT_READ) {
    filter |= EVFILT_READ;
  }
  if (op & EVENT_WRITE) {
    filter |= EVFILT_WRITE;
  }
  EV_SET(&event, fileno, filter, EV_ADD, 0, 0, data);
  kevent(sel->kq, &event, 1, &event, 0, NULL);
}

void Selector_unregister(Selector *sel, int fileno, int op) {
  struct kevent event;
  int filter;
  if (op == EVENT_READ) {
    filter = EVFILT_READ;
  } 
  if (op == EVENT_WRITE) {
    filter = EVFILT_WRITE;
  }
  EV_SET(&event, fileno, filter, EV_DELETE, 0, 0, 0);
  kevent(sel->kq, &event, 1, &event, 0, NULL);
}

int Selector_select(Selector *sel, IOEvent *ioevents, int maxevents, _PyTime_t timeout) {
  /* Wait for events */
  int nev, n, op;
  struct kevent events[MAX_EVENT];
  struct kevent *evt;
  struct timespec ts, *tsp;

  if (timeout) {
    _PyTime_AsTimespec(timeout, &ts);
    tsp = &ts;
  } else {
    tsp = NULL;
  }

  maxevents = maxevents < MAX_EVENT ? maxevents : MAX_EVENT;

  Py_BEGIN_ALLOW_THREADS
  nev = kevent(sel->kq, events, 0, events, maxevents, tsp);
  Py_END_ALLOW_THREADS

  for (n = 0, evt=events; n < nev; n++, evt++, ioevents++) {
    op = 0;
    if (evt->filter == EVFILT_READ) {
      op |= EVENT_READ;
    }
    if (evt->filter == EVFILT_WRITE) {
      op |= EVENT_WRITE;
    }
    ioevents->op = op;
    ioevents->fileno = evt->ident;
    ioevents->data = evt->udata;
  }
  return nev;
}

#endif

#ifdef HAVE_EPOLL

#include <sys/epoll.h>

#define MAX_EVENT 64
typedef struct Selector {
  int efd;
  void **data;
  int datamax;
} Selector;

static Selector *
new_Selector(void) {
  Selector *sel;
  sel = (Selector *) malloc(sizeof(Selector));
  sel->efd = epoll_create1(0);
  sel->data = (void **) malloc(MAX_EVENT*sizeof(void *));
  sel->datamax = MAX_EVENT;
  return sel;
}

static void
delete_Selector(Selector *sel) {
  close(sel->efd);
  free(sel->data);
  free(sel);
}

static void 
Selector_register(Selector *sel, int op, int fileno, void *data) {
  struct epoll_event ev;
  unsigned int events = EPOLLET;

  printf("register %d\n", fileno);
  if (op & EVENT_READ) {
    events |= EPOLLIN;
  }
  if (op & EVENT_WRITE) {
    events |= EPOLLOUT;
  }
  while (fileno >= sel->datamax) {
    sel->data = (void **) realloc(sel->data, 2*sel->datamax*sizeof(void *));
    sel->datamax *= 2;
  }
  sel->data[fileno] = data;
  ev.events = events;
  ev.data.fd = fileno;
  epoll_ctl(sel->efd, EPOLL_CTL_ADD, fileno, &ev);
}

void Selector_unregister(Selector *sel, int fileno, int op) {
  struct epoll_event ev;
  printf("unregister %d\n", fileno);
  ev.events = 0;
  ev.data.fd = fileno;
  epoll_ctl(sel->efd, EPOLL_CTL_DEL, fileno, &ev);
}

int Selector_select(Selector *sel, IOEvent *ioevents, int maxevents, _PyTime_t timeout) {
  /* Wait for events */
  int nev, n, op;
  struct epoll_event events[MAX_EVENT];
  struct epoll_event *evt;
  _PyTime_t ms = -1;

  if (timeout) {
    ms = _PyTime_AsMilliseconds(timeout, _PyTime_ROUND_CEILING);
  }

  maxevents = maxevents < MAX_EVENT ? maxevents : MAX_EVENT;

  Py_BEGIN_ALLOW_THREADS
    nev = epoll_wait(sel->efd, &events[0], maxevents, (int) ms);
  Py_END_ALLOW_THREADS

  for (n = 0, evt=events; n < nev; n++, evt++, ioevents++) {
    op = 0;
    if (evt->events & EPOLLIN) {
      op |= EVENT_READ;
    }
    if (evt->events & EPOLLOUT) {
      op |= EVENT_WRITE;
    }
    ioevents->op = op;
    ioevents->fileno = evt->data.fd;
    ioevents->data = sel->data[evt->data.fd];
  }
  return nev;
}

#endif

/* Forward declaration */

typedef struct TaskQueue {
  struct CTask *first;
  struct CTask *last;
} TaskQueue;

static TaskQueue *new_TaskQueue(void);
static void delete_TaskQueue(TaskQueue *);

/* -----------------------------------------------------------------------------
 * Tasks.   Defined as a Python object to assist with memory management and
 * other details.
 * ----------------------------------------------------------------------------- */

struct CTask {
  PyObject_HEAD
  struct CTask *next;
  struct CTask *prev;
  int       taskid;
  int       terminated;
  int       cancelled;
  PyObject *state;
  int       daemon;
  int       cycles;
  PyObject  *coro;       
  PyObject  *coro_send;
  PyObject  *coro_throw;
  PyObject  *next_value;
  PyObject  *next_exc;
  TaskQueue *joining;
  int       last_io_op;
  int       last_io_fd;

  /* Time management */
  _PyTime_t  sleep;       /* Current sleep value */
  _PyTime_t  timeout;     /* Current timeout value */

  /* Cancellation */
  cancelfunc_t    cancelfunc;
  int             current_fd;       /* Current fd used for I/O */
  int             current_op;       /* Current IO operation */
  PyObject       *wait_queue;
  PyObject       *future;
  PyObject       *sigset;
  struct CTask          *join_task;

};

/* Create a new CTask object wrapped around a coroutine */
CTask *
new_CTask(PyObject *coro, PyTypeObject *type) {
  CTask *task;
  task = (CTask *) (type->tp_alloc(type, 0));
  if (!task) {
    return NULL;
  }
  task->taskid = 0;
  task->terminated = 0;
  task->cancelled = 0;
  task->state = NULL;
  task->daemon = 0;
  task->cycles = 0;
  task->next = NULL;
  task->prev = NULL;
  task->coro = coro;
  task->cancelfunc = NULL;
  task->wait_queue = NULL;
  task->future = NULL;
  task->sigset = NULL;

  Py_INCREF(coro);
  task->coro_send = PyObject_GetAttrString(coro, "send");
  task->coro_throw = PyObject_GetAttrString(coro, "throw");
  task->next_value = NULL;
  task->next_exc = NULL;
  task->joining = new_TaskQueue();
  task->last_io_op = -1;
  task->last_io_fd = -1;
  return task;
}

void 
delete_CTask(CTask *task) {
  printf("Deleting CTask %x\n", task);
  Py_DECREF(task->coro);
  Py_DECREF(task->coro_send);
  Py_DECREF(task->coro_throw);
  Py_XDECREF(task->state);
  Py_XDECREF(task->next_value);
  Py_XDECREF(task->next_exc);
  Py_XDECREF(task->wait_queue);
  Py_XDECREF(task->future);
  Py_XDECREF(task->sigset);
  delete_TaskQueue(task->joining);
}

static PyObject *
ctask_new(PyTypeObject *type, PyObject *args, PyObject *kw)
{
  PyObject *coro;
  CTask *self;
  int daemon = 0;

  if (!PyArg_ParseTuple(args, "O|i", &coro, &daemon)) {
    return NULL;
  }
  self = new_CTask(coro, type);
  self->daemon = daemon;
  return (PyObject *) self;
}

#define OFFSET(field)  offsetof(CTask, field)

static PyMemberDef ctask_members[] = {
  {"id",         T_INT, OFFSET(taskid),         READONLY,
   PyDoc_STR("Task id.")},

  {"terminated", T_INT, OFFSET(terminated),         READONLY,
   PyDoc_STR("Task terminated?")},

  {"cycles",         T_INT, OFFSET(cycles),         READONLY,
   PyDoc_STR("Number of execution cycles.")},

  {"daemon",         T_INT, OFFSET(daemon),         READONLY,
   PyDoc_STR("Daemonic flag.")},

  {"coro",      T_OBJECT, OFFSET(coro),      READONLY,
   PyDoc_STR("Underlying coroutine.")},

  {"state",     T_OBJECT, OFFSET(state), READONLY,
   PyDoc_STR("Current state.")},

  {"next_value", T_OBJECT, OFFSET(next_value), READONLY,
   PyDoc_STR("Last value.")},

  {"next_exc", T_OBJECT, OFFSET(next_exc), READONLY,
   PyDoc_STR("Exception info.")},

  {NULL}
};


static PyTypeObject Colonel_TaskType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  "colonel.CTask",                               /* tp_name */
  sizeof(CTask),                                /* tp_basicsize */
  0,                                                  /* tp_itemsize */
  (destructor) delete_CTask,                          /* tp_dealloc */
  0,                                                  /* tp_print */
  0,                                                  /* tp_getattr */
  0,                                                  /* tp_setattr */
  0,                                                  /* tp_reserved */
  0,                                                  /* tp_repr */
  0,                                                  /* tp_as_number */
  0,                                                  /* tp_as_sequence */
  0,                                                  /* tp_as_mapping */
  0,                                                  /* tp_hash */
  0,                                                  /* tp_call */
  0,                                                  /* tp_str */
  PyObject_GenericGetAttr,                            /* tp_getattro */
  0,                                                  /* tp_setattro */
  0,                                                  /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,           /* tp_flags */
  0,                                                  /* tp_doc */
  0,                                                  /* tp_traverse */
  0,                                                  /* tp_clear */
  0,                                                  /* tp_richcompare */
  0,                                                  /* tp_weaklistoffset */
  0,                                                  /* tp_iter */
  0,                                                  /* tp_iternext */
  0,                                                  /* tp_methods */
  ctask_members,                                      /* tp_members */
  0,                                                  /* tp_getset */
  0,                                                  /* tp_base */
  0,                                                  /* tp_dict */
  0,                                                  /* tp_descr_get */
  0,                                                  /* tp_descr_set */
  0,                                                  /* tp_dictoffset */
  0,                                                  /* tp_init */
  0,                                                  /* tp_alloc */
  ctask_new,                                          /* tp_new */
  0,                                                  /* tp_free */
};

/* -----------------------------------------------------------------------------
   Queues
   ----------------------------------------------------------------------------- */

static TaskQueue *
new_TaskQueue(void) {
  TaskQueue *q;
  q = (TaskQueue *) malloc(sizeof(TaskQueue));
  if (!q) {
    return NULL;
  }
  q->first = NULL;
  q->last = NULL;
  return q;
}

static void
delete_TaskQueue(TaskQueue *q) {
  free(q);
}

static void
TaskQueue_push(TaskQueue *q, CTask *task) {
  task->prev = q->last;
  if (q->first) {
    q->last->next = task;
  } else {
    q->first = task;
  }
  q->last = task;
}

static CTask *
TaskQueue_pop(TaskQueue *q) {
  CTask *task = q->first;
  if (task) {
    q->first = task->next;
    if (q->first) {
      q->first->prev = NULL;
    }
    if (!task->next) {
      q->last = NULL;
    }
    task->next = NULL;
    task->prev = NULL;
  } 
  return task;
}

void
TaskQueue_remove(TaskQueue *q, CTask *task) {
  if (task->prev) {
    task->prev->next = task->next;
  } else {
    q->first = task->next;
  }
  if (!task->next) {
    q->last = task->prev;
  }
  task->next = NULL;
  task->prev = NULL;
}

/* -----------------------------------------------------------------------------
 * Kernel
 * ----------------------------------------------------------------------------- */

struct Kernel {
  PyObject  *pykernel;
  TaskQueue *ready;
  Selector  *selector;
  int        ntasks;
  PyObject  *task_factory;
  int        last_task_id;
  Timeout   *timeouts;
  int        loopback_init;
  int        signal_init;
  int        wake_fd;
  PyObject  *wake_queue;
};

Kernel *
new_Kernel(PyObject *pykernel, PyObject *task_factory) {
  Kernel *kernel;

  kernel = (Kernel *) malloc(sizeof(Kernel));
  if (!kernel) {
    return NULL;
  }
  kernel->pykernel = pykernel;
  Py_INCREF(kernel->pykernel);
  kernel->ready = new_TaskQueue();
  kernel->selector = new_Selector();
  kernel->ntasks = 0;
  kernel->task_factory = task_factory;
  kernel->last_task_id = 1;
  kernel->timeouts = NULL;
  kernel->wake_fd = -1;

  /* Fix this. Pull attributes from pykernel to find out status */
  {
    PyObject *obj = PyObject_GetAttrString(pykernel, "_wait_sock");
    kernel->loopback_init = (obj == Py_None) ? 0 : 1;
    if (obj != Py_None) {
      PyObject *fdobj = PyObject_CallMethod(obj, "fileno","");
      kernel->wake_fd = PyLong_AsLong(fdobj);
      Selector_register(kernel->selector, EVENT_READ, kernel->wake_fd, NULL);
      Py_DECREF(fdobj);
    }
    Py_DECREF(obj);
  }
  {
    PyObject *obj = PyObject_GetAttrString(pykernel, "_signal_sets");
    kernel->signal_init = (obj == Py_None) ? 0 : 1;
    Py_DECREF(obj);
  }
  kernel->wake_queue = PyObject_GetAttrString(pykernel, "_wake_queue");
  Py_INCREF(kernel->task_factory);
  return kernel;
}

void
delete_Kernel(Kernel *kernel) {
  printf("Deleting KernelState\n");
  if (kernel->wake_fd > 0) {
    Selector_unregister(kernel->selector, kernel->wake_fd, EVENT_READ);
  }
  delete_TaskQueue(kernel->ready);
  delete_Selector(kernel->selector);
  Py_XDECREF(kernel->task_factory);
  Py_XDECREF(kernel->pykernel);
  Py_XDECREF(kernel->wake_queue);
  free(kernel);
}

static Kernel *
PyKernel_AsKernel(PyObject *obj) {
  return (Kernel *) PyCapsule_GetPointer(obj, "KernelState");
}

static void del_PyKernel(PyObject *obj) {
  delete_Kernel((Kernel *) PyCapsule_GetPointer(obj, "KernelState"));
}

static PyObject *
PyKernel_FromKernel(Kernel *kernel, int must_free) {
  return PyCapsule_New(kernel, "KernelState", must_free ? del_PyKernel : NULL);
}

/* Put a task on the ready queue */
void
Kernel_schedule_ready(Kernel *kernel, CTask *task) {
  assert(!task->terminated);
  TaskQueue_push(kernel->ready, task);
  task->cancelfunc = NULL;
}

/* Return the next ready task */
CTask *
Kernel_next_ready(Kernel *kernel) {
  return TaskQueue_pop(kernel->ready);
}

/* Create a new Task object using the kernel task factory */
CTask *
Kernel_new_task(Kernel *kernel, PyObject *coro, PyObject *daemon) {
  CTask *task;

  task = (CTask *) PyObject_CallFunctionObjArgs(kernel->task_factory, coro, daemon, NULL);
  task->taskid = kernel->last_task_id++;
  return task;
}

/* Create a new timeout in the kernel */
Timeout *
Kernel_new_timeout(Kernel *kernel, CTask *task, _PyTime_t timeout, int type) {
  Timeout *t, *next;
  Timeout **prev = &kernel->timeouts;

  t = new_Timeout(timeout, task, type);
  next = kernel->timeouts;
  while (next) {
    if (t->timeout <= next->timeout) {
      break;
    }
    prev = &next->next;
    next = next->next;
  }

  t->next = next;
  *prev = t;
  return t;
}

int Kernel_cancel_task(Kernel *kernel, CTask *task, PyObject *exc) {
  PyObject *val;
  if (task->terminated) {
    return 1;          /* Task is already terminated */
  }
  if (!task->cancelfunc) {
    /* No cancellation function is currently set.  This means the task is
       sitting on the ready queue.
    */
    return 0;
  }

  /* Trigger the cancellation function */
  (*task->cancelfunc)(kernel, task);

  /* Create an exception */
  val = PyObject_CallFunctionObjArgs(exc, NULL);
  task->next_exc = val;
  Kernel_schedule_ready(kernel, task);
  return 1;
}

void Kernel_init_loopback(Kernel *kernel) {
  PyObject *fdobj;
  if (kernel->loopback_init) {
    return;
  }
  fdobj = PyObject_CallMethod(kernel->pykernel, "_init_loopback", "");
  kernel->loopback_init = 1;
  kernel->wake_fd = PyLong_AsLong(fdobj);

  Py_DECREF(fdobj);
  Selector_register(kernel->selector, EVENT_READ, kernel->wake_fd, NULL);
}

void Kernel_init_signals(Kernel *kernel) {
  PyObject *result;
  if (kernel->signal_init) {
    return;
  }
  result = PyObject_CallMethod(kernel->pykernel, "_init_signals", "");
  kernel->signal_init = 1;
  Py_DECREF(result);
}

/* -----------------------------------------------------------------------------
 * traps
 * ----------------------------------------------------------------------------- */

typedef int (*trapfunc)(Kernel *, CTask *, PyObject *);

static void _io_cancel(Kernel *kernel, CTask *task) {
  Selector_unregister(kernel->selector, task->current_fd, task->current_op);
  task->current_fd = -1;
  task->current_op = -1;
}

static int _trap_io(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *fileobj;
  int op;
  int fd;
  PyObject *statename;

  if (!PyArg_ParseTuple(args, "OOiO", &trapnum, &fileobj, &op, &statename)) {
    return 0;
  }

  fd = PyObject_AsFileDescriptor(fileobj);
  if (fd == -1) {
    return 0;
  }
  if ((task->last_io_op != op) || (task->last_io_fd != fd)) {
    if (task->last_io_fd > 0) {
      Selector_unregister(kernel->selector, task->last_io_fd, task->last_io_op);
    }
    Selector_register(kernel->selector, op, fd, (void *) task);
  }
  task->current_fd = fd;
  task->current_op = op;
  task->cancelfunc = _io_cancel;
  task->last_io_op = -1;
  task->last_io_fd = -1;
  return 1;
}


static void _future_cancel(Kernel *kernel, CTask *task) {
  PyObject_CallMethod(task->future, "cancel", "");
  Py_DECREF(task->future);
  task->future = 0;
}

static int _trap_future_wait(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *future;
  PyObject *event;
  PyObject *result;

  if (!PyArg_ParseTuple(args, "OOO", &trapnum, &future, &event)) {
    return 0;
  }

  Kernel_init_loopback(kernel);  
  task->future = future;
  Py_INCREF(task->future);
  task->cancelfunc = _future_cancel;

  result = PyObject_CallMethod(kernel->pykernel, "_add_future_callback", "OO", future, task);
  Py_DECREF(result);

  if (event != Py_None) {
    result = PyObject_CallMethod(event, "set", "");
    Py_DECREF(result);
  }
  return 1;
}


static void _sleep_cancel(Kernel *kernel, CTask *task) {
  task->sleep = 0;
}

static int _trap_sleep(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *timeobj = Py_None;
  _PyTime_t seconds, clock;

  if (!PyArg_ParseTuple(args, "OO", &trapnum, &timeobj)) {
    return 0;
  }
  if (_PyTime_FromSecondsObject(&seconds, timeobj, _PyTime_ROUND_CEILING) < 0) {
    return 0;
  }
  clock = _PyTime_GetMonotonicClock();
  task->sleep = clock + seconds;
  Kernel_new_timeout(kernel, task, task->sleep, TIMEOUT_SLEEP);
  task->cancelfunc = _sleep_cancel;
  return 1;
}
    
static int _trap_spawn(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *coro;
  PyObject *daemon;
  CTask *child;
  if (!PyArg_ParseTuple(args, "OOO", &trapnum, &coro, &daemon)) {
    return 0;
  }
  child = Kernel_new_task(kernel, coro, daemon);
  Py_INCREF(child);   /* Kernel needs an extra reference as long as task is active */
  task->next_value = (PyObject *) child;
  Kernel_schedule_ready(kernel, child);
  Kernel_schedule_ready(kernel, task);
  kernel->ntasks++;
  return 1;
}

static void _join_cancel(Kernel *kernel, CTask *task) {
  TaskQueue_remove(task->join_task->joining, task);
  Py_DECREF(task->join_task);
  task->join_task = NULL;
}

static int _trap_join(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *taskobj;
  CTask *child;

  if (!PyArg_ParseTuple(args, "OO", &trapnum, &taskobj)) {
    return 0;
  }

  child = (CTask *) taskobj;
  if (!child) {
    return 0;
  }
  if (child->terminated) {
    TaskQueue_push(kernel->ready, task);
  } else {
    TaskQueue_push(child->joining, task);
    task->join_task = child;
    Py_INCREF(task->join_task);
    task->cancelfunc = _join_cancel;
  }
  return 1;
}

static int _trap_cancel(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  CTask *child;
  if (!PyArg_ParseTuple(args, "OO", &trapnum, &child)) {
    return 0;
  }
  if (child == task) {
    /* Task can't cancel itself.  Raise an error */
    return 1;
  }
  if (child->cancelled || Kernel_cancel_task(kernel, child, Curio_CancelledError)) {
    child->cancelled = 1;
    if (child->terminated) {
      Kernel_schedule_ready(kernel, task);
    } else {
      TaskQueue_push(child->joining, task);
      task->join_task = child;
      Py_INCREF(task->join_task);
      task->cancelfunc = _join_cancel;
    }
  } else {
    task->next_exc = PyObject_CallFunctionObjArgs(Curio_CancelRetry, NULL);
    Kernel_schedule_ready(kernel, task);
  }
  return 1;
}


static void _wait_queue_cancel(Kernel *kernel, CTask *task) {
  PyObject_CallMethodObjArgs(task->wait_queue, REMOVE_METHOD, task, NULL);
  Py_DECREF(task->wait_queue);
  task->wait_queue = NULL;
}

static int _trap_wait_queue(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *queue;
  PyObject *state;
  PyObject *result;

  if (!PyArg_ParseTuple(args, "OOO", &trapnum, &queue, &state)) {
    return 0;
  }
  Py_XDECREF(task->state);
  task->state = state;
  Py_INCREF(state);
  
  task->cancelfunc = _wait_queue_cancel;
  task->wait_queue = queue;

  Py_INCREF(task->wait_queue);
  result = PyObject_CallMethodObjArgs(queue, APPEND_METHOD, task, NULL);
  Py_XDECREF(result);
  return 1;
}

static int _trap_reschedule_tasks(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *queue;
  int n;
  PyObject *value;
  PyObject *exc;

  if (!PyArg_ParseTuple(args, "OOiOO", &trapnum, &queue, &n, &value, &exc)) {
    return 0;
  }
  while (n > 0) {
    CTask *child = (CTask *) PyObject_CallMethodObjArgs(queue, POPLEFT_METHOD, NULL);
    if (exc == Py_None) {
      child->next_value = value;
      Py_INCREF(child->next_value);
      child->next_exc = NULL;
    } else {
      child->next_exc = exc;
      Py_INCREF(child->next_exc);
      child->next_value = NULL;
    }
    Py_XDECREF(child->wait_queue);
    child->cancelfunc = NULL;
    Kernel_schedule_ready(kernel, child);
    Py_DECREF(child);
    n--;
  }
  Kernel_schedule_ready(kernel, task);
  return 1;
}

static int _trap_sigwatch(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *sigset;
  PyObject *result;

  if (!PyArg_ParseTuple(args, "OO", &trapnum, &sigset)) {
    return 0;
  }
  
  Kernel_init_loopback(kernel);
  Kernel_init_signals(kernel);

  result = PyObject_CallMethod(kernel->pykernel, "_signal_watch", "O", sigset);
  if (!result) {
    return 0;
  }
  Py_DECREF(result);
  Kernel_schedule_ready(kernel, task);
  return 1;
}

static int _trap_sigunwatch(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *sigset;
  PyObject *result;

  if (!PyArg_ParseTuple(args, "OO", &trapnum, &sigset)) {
    return 0;
  }
  result = PyObject_CallMethod(kernel->pykernel, "_signal_unwatch", "O", sigset);
  if (!result) {
    return 0;
  }
  Py_DECREF(result);
  Kernel_schedule_ready(kernel, task);
  return 1;
}

static void _sigwait_cancel(Kernel *kernel, CTask *task) {
  PyObject_SetAttrString(task->sigset, "waiting", Py_None);
  Py_DECREF(task->sigset);
  task->sigset = NULL;
  task->cancelfunc = NULL;
}

static int _trap_sigwait(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *sigset;

  if (!PyArg_ParseTuple(args, "OO", &trapnum, &sigset)) {
    return 0;
  }
  PyObject_SetAttrString(sigset, "waiting", (PyObject *) task);
  task->sigset = sigset;
  task->cancelfunc = _sigwait_cancel;
  return 1;
}

static int _trap_get_kernel(Kernel *kernel, CTask *task, PyObject *args) {
  task->next_value = kernel->pykernel;
  Py_INCREF(task->next_value);
  Kernel_schedule_ready(kernel, task);
  return 1;
}

static int _trap_get_current(Kernel *kernel, CTask *task, PyObject *args) {
  task->next_value = (PyObject *) task;
  Py_INCREF(task->next_value);
  Kernel_schedule_ready(kernel, task);
  return 1;
}

static int _trap_set_timeout(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *timeobj;
  _PyTime_t seconds, clock;

  _PyTime_t old_timeout = task->timeout;

  if (!PyArg_ParseTuple(args, "OO", &trapnum, &timeobj)) {
    return 0;
  }
  old_timeout = task->timeout;
  if (timeobj != Py_None) {
    _PyTime_t new_timeout;

    if (_PyTime_FromSecondsObject(&seconds, timeobj, _PyTime_ROUND_CEILING) < 0) {
      return 0;
    }
    clock = _PyTime_GetMonotonicClock();
    new_timeout = clock + seconds;
    if (!task->timeout || (new_timeout < task->timeout)) {
      task->timeout = new_timeout;
      Kernel_new_timeout(kernel, task, task->timeout, TIMEOUT_TIMEOUT);
    }
  } else {
    task->timeout = 0;
  }
  if (old_timeout) {
    task->next_value = PyFloat_FromDouble(_PyTime_AsSecondsDouble(task->timeout));
  } else {
    task->next_value = Py_None;
    Py_INCREF(task->next_value);
  }
  Kernel_schedule_ready(kernel, task);
  return 1;
}

static int _trap_unset_timeout(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *timeobj;
  _PyTime_t seconds, clock;

  if (!PyArg_ParseTuple(args, "OO", &trapnum, &timeobj)) {
    return 0;
  }
  if (timeobj == Py_None) {
    task->timeout = 0;
  } else {
    if (_PyTime_FromSecondsObject(&seconds, timeobj, _PyTime_ROUND_CEILING) < 0) {
      return 0;
    }
    clock = _PyTime_GetMonotonicClock();
    if (seconds < clock) {
      task->timeout = clock;
      Kernel_new_timeout(kernel, task, clock, TIMEOUT_TIMEOUT);
    } else {
      task->timeout = seconds;
    }
  }
  task->next_value = Py_None;
  Py_INCREF(Py_None);
  Kernel_schedule_ready(kernel, task);
  return 1;
}

static trapfunc trap_table[15] = {
  _trap_io,                /* 0 - trap_io */
  _trap_future_wait,       /* 1 - future_wait */
  _trap_sleep,             /* 2 - sleep */
  _trap_spawn,             /* 3 - spawn */
  _trap_cancel,            /* 4 - cancel */
  _trap_join,              /* 5 - join */
  _trap_wait_queue,        /* 6 - wait_queue */
  _trap_reschedule_tasks,  /* 7 - reschedule_tasks */
  _trap_sigwatch,          /* 8 - sigwatch */
  _trap_sigunwatch,        /* 9 - sigunwatch*/
  _trap_sigwait,           /* 10 - sigwait */
  _trap_get_kernel,        /* 11 - get_kernel */
  _trap_get_current,       /* 12 - get_current */
  _trap_set_timeout,       /* 13 - set_timeout */
  _trap_unset_timeout,     /* 14 - unset_timeout */
};

/* Cleanup a task that's terminated */
static void
cleanup_task(Kernel *kernel, CTask *task) {
  CTask *child;

  task->terminated = 1;

  if (task->last_io_fd >= 0) {
    Selector_unregister(kernel->selector, task->last_io_fd, task->last_io_op);
    task->last_io_fd = -1;
    task->last_io_op = 0;
  }

  /* Put all joining tasks back on ready queue */
  while (1) {
    child = TaskQueue_pop(task->joining);
    if (!child) {
      break;
    }
    TaskQueue_push(kernel->ready, child);
  }
  Py_DECREF(task);
  kernel->ntasks--;
}

static void
handle_wake_events(Kernel *kernel) {
  unsigned char buffer[256];
  int  nread, n;

  do {
    nread = recv(kernel->wake_fd, buffer, 256, 0);
    if (nread > 0) {
      for (n = 0; n < nread; n++) {
	if (buffer[n] == 0) {
	  /* Put an item back on the ready queue */
	  PyObject *wake_tuple, *future;
	  CTask *task;
	  wake_tuple = PyObject_CallMethodObjArgs(kernel->wake_queue, POPLEFT_METHOD, NULL);
	  task = (CTask *) PySequence_GetItem(wake_tuple, 0);
	  future = PySequence_GetItem(wake_tuple, 1);
	  if (task->future == future) {
	    Py_DECREF(task->future);
	    task->future = NULL;
	    task->cancelfunc = NULL;
	    Kernel_schedule_ready(kernel, task);
	  }
	  Py_DECREF(task);
	  Py_DECREF(future);
	  Py_DECREF(wake_tuple);
	} else {
	  PyObject *waking;
	  int m, sz;
	  waking = PyObject_CallMethod(kernel->pykernel, "_signal_helper", "i", buffer[n]);
	  sz = PySequence_Size(waking);
	  for (m = 0; m < sz; m++) {
	    CTask *task = (CTask *) PySequence_GetItem(waking, m);
	    task->next_value = Py_None;
	    Py_INCREF(Py_None);
	    Kernel_schedule_ready(kernel, task);
	    Py_DECREF(task);
	  }
	  Py_DECREF(waking);
	}
      }
    }
  } while (nread > 0);
}
static int 
poll_for_io(Kernel *kernel) {
  IOEvent ioevents[MAX_EVENT];
  int nev, n;
  CTask *task;
  _PyTime_t timeout = 0;
  if (kernel->timeouts) {
    _PyTime_t clock = _PyTime_GetMonotonicClock();
    if (clock > kernel->timeouts->timeout) {
      timeout = 1;
    } else {
      timeout = kernel->timeouts->timeout - clock;
    }
  }
  nev = Selector_select(kernel->selector, ioevents, MAX_EVENT, timeout);
  for (n = 0; n < nev; n++) {
    task = (CTask *) ioevents[n].data;
    if (task) {
      Kernel_schedule_ready(kernel, task);
      task->last_io_op = ioevents[n].op;
      task->last_io_fd = ioevents[n].fileno;
    } else if (ioevents[n].fileno == kernel->wake_fd) {
      handle_wake_events(kernel);
    }
  }
  return nev;
}

static void
handle_sleeping(Kernel *kernel) {
  _PyTime_t clock;
  Timeout *t, **first;

  clock = _PyTime_GetMonotonicClock();
 
  first = &kernel->timeouts;
  t = *first;
  while (t && t->timeout <= clock) {
    /* Check if the associated task timeout matches */
    if ((t->type == TIMEOUT_SLEEP) && (t->task->sleep == t->timeout)) {
      Kernel_schedule_ready(kernel, t->task);
      t->task->sleep = 0;
    } else if ((t->type == TIMEOUT_TIMEOUT) && (t->task->timeout == t->timeout)) {
      if (Kernel_cancel_task(kernel, t->task, Curio_TaskTimeout)) {
	t->task->timeout = 0;
      } else {
	/* The cancel failed.  We leave the timeout on the queue for a later retry */
	first = &t->next;
	t = 0;
      }
    }
    if (t) {
      *first = t->next;
      delete_Timeout(t);
    }
    t = *first;
  }
}

/* -----------------------------------------------------------------------------
 * Core Kernel Loop
 * ----------------------------------------------------------------------------- */

PyObject *
Kernel_run(Kernel *kernel, PyObject *coro) {
  CTask *task;
  PyObject *request;

  task = Kernel_new_task(kernel, coro, NULL);
  Kernel_schedule_ready(kernel, task);
  kernel->ntasks++;

  while (kernel->ntasks > 0) {
    task = Kernel_next_ready(kernel);
    if (!task) {
      if (poll_for_io(kernel) < 0) {
	if (PyErr_CheckSignals()) {
	  goto error;
	}
      }
      handle_sleeping(kernel);
      continue;
    }
    /* Run the coroutine */
    if (task->next_exc) {
      request = PyObject_CallFunctionObjArgs(task->coro_throw, task->next_exc, NULL);
      Py_DECREF(task->next_exc);
      task->next_exc = NULL;
    } else {
      request = PyObject_CallFunctionObjArgs(task->coro_send, task->next_value ? task->next_value : Py_None, NULL);
      Py_XDECREF(task->next_value);
      task->next_value = NULL;
    }
    if (!PyErr_Occurred()) {
      int trapnum;
      PyObject *pytrapnum;

      /*
      printf("REQUEST: %x ", task);
       PyObject_Print(request, stdout, 0);
       printf("\n");
      */

      if (!PyTuple_Check(request)) {
	/* No tuple passed */
	PyErr_SetString(PyExc_RuntimeError, "Trap must be a tuple");
	goto trap_fail;
      }
      pytrapnum = PyTuple_GetItem(request, 0);
      if (!pytrapnum) {
	/* No trap number */
	PyErr_SetString(PyExc_RuntimeError, "No trap arguments given");
	goto trap_fail;
      }
      if (!PyLong_Check(pytrapnum)) {
	/* Not an integer */
	PyErr_SetString(PyExc_RuntimeError, "Trap number must be an integer");
	goto trap_fail;
      }
      trapnum = PyLong_AsLong(pytrapnum);
      if (!trap_table[trapnum]) {
	/* No trap function defined */
	PyErr_SetString(PyExc_RuntimeError, "No trap handler function defined");
	goto trap_fail;
      } else {
	int status = trap_table[trapnum](kernel, task, request);
	if (task->last_io_fd >= 0) {
	  Selector_unregister(kernel->selector, task->last_io_fd, task->last_io_op);
	  task->last_io_fd = -1;
	  task->last_io_op = 0;
	}
	if (!status) {
	  /* Trap function failed */
	  if (!PyErr_Occurred()) {
	    PyErr_SetString(PyExc_RuntimeError, "Trap handler failed. Reasons unknown");
	  }
	  goto trap_fail;
	}
      }
      /* Successful trap execution */
      continue;

    trap_fail:
      Py_DECREF(request);
      return NULL;
    } else {
      PyObject *type, *value, *traceback;
      PyErr_Fetch(&type, &value, &traceback);
      PyErr_NormalizeException(&type, &value, &traceback);
      PyErr_Restore(type, value, traceback);
      if (PyErr_ExceptionMatches(PyExc_StopIteration)) {
	task->next_value = value ? PyObject_GetAttrString(value, "value") : NULL;
	printf("Task Done %x\n", task);
      } else if (PyErr_ExceptionMatches(Curio_CancelledError)) {
	task->next_value = Py_None;
	Py_INCREF(task->next_value);
	task->next_exc = NULL;
      } else {
	printf("Task Crash %x\n", task);
	task->next_exc = value;
	Py_INCREF(value);
	PyErr_PrintEx(0);
      }
      /* Clean up the terminated task */
      cleanup_task(kernel, task);
      PyErr_Clear();
    }
  }
  return Py_BuildValue("");
 error:
  return NULL;
}

PyObject *
py_run(PyObject *self, PyObject *args) {
  PyObject *kernelstate;
  PyObject *coro;
  PyObject *result;
  Kernel   *kernel;

  if (!PyArg_ParseTuple(args, "OO", &kernelstate, &coro)) {
    return NULL;
  }

  kernel = PyKernel_AsKernel(kernelstate);
  if (!kernel) {
    return NULL;
  }
  result = Kernel_run(kernel, coro);
  return result;
}

PyObject *
py_kernelstate(PyObject *self, PyObject *args) {
  PyObject *pykernel;
  PyObject *task_factory;
  PyObject *result;
  Kernel *kernel;

  if (!PyArg_ParseTuple(args, "OO", &pykernel, &task_factory)) {
    return NULL;
  }
  kernel = new_Kernel(pykernel, task_factory);
  return PyKernel_FromKernel(kernel, 1);
}

static PyMethodDef ColonelMethods[] = {
  { "run", py_run, METH_VARARGS, NULL },
  { "kernelstate", py_kernelstate, METH_VARARGS, NULL },
  { NULL, NULL, 0, NULL },
};

static struct PyModuleDef _colonelmodule = {
  PyModuleDef_HEAD_INIT, 
  "_colonel",
  "_colonel",
  -1,
  ColonelMethods,
  NULL,
  NULL,
  NULL,
  NULL
};

PyMODINIT_FUNC
PyInit__colonel(void) {
  PyObject *m;
  m = PyModule_Create(&_colonelmodule);
  if (!m) {
    return NULL;
  }

  if (PyType_Ready(&Colonel_TaskType) < 0) {
    return NULL;
  }

  Py_INCREF(&Colonel_TaskType);
  PyModule_AddObject(m, "CTask", (PyObject *) &Colonel_TaskType);

  /* Import curio and pull important information about exceptions and other things */
  {
    PyObject *curio_errors = PyImport_ImportModule("curio.errors");
    if (!curio_errors) {
      printf("Couldn't import curio.errors");
    } else {
      Curio_CancelledError = PyObject_GetAttrString(curio_errors, "CancelledError");
      Curio_CancelRetry = PyObject_GetAttrString(curio_errors, "_CancelRetry");
      Curio_TaskTimeout = PyObject_GetAttrString(curio_errors, "TaskTimeout");
      Curio_TaskError = PyObject_GetAttrString(curio_errors, "TaskError");
    }
  }
  APPEND_METHOD = PyUnicode_FromString("append");
  REMOVE_METHOD = PyUnicode_FromString("remove");
  POPLEFT_METHOD = PyUnicode_FromString("popleft");

  return m;
}

