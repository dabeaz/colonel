/* colonel.c

   Pure C implementation of the Curio kernel.  Highly experimental.
*/

#include "Python.h"
#include "structmember.h"

#include <unistd.h>

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
  printf("register %d\n", fileno);
}

void Selector_unregister(Selector *sel, int fileno) {
  struct kevent event;
  EV_SET(&event, fileno, 0, EV_DELETE, 0, 0, 0);
  kevent(sel->kq, &event, 1, &event, 0, NULL);
  printf("unregister %d\n", fileno);
}

int Selector_select(Selector *sel, IOEvent *ioevents, int maxevents) {
  /* Wait for events */
  int nev, n, op;
  struct kevent events[MAX_EVENT];
  struct kevent *evt;

  maxevents = maxevents < MAX_EVENT ? maxevents : MAX_EVENT;

  Py_BEGIN_ALLOW_THREADS
  nev = kevent(sel->kq, events, 0, events, maxevents, NULL);
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
#echo "HAS EPOLL"
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

typedef struct CTask {
  PyObject_HEAD
  struct CTask *next;            
  int       taskid;
  int       terminated;
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
} CTask;

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
  task->state = NULL;
  task->daemon = 0;
  task->cycles = 0;
  task->next = NULL;
  task->coro = coro;

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
    if (!task->next) {
      q->last = NULL;
    }
    task->next = NULL;
  } 
  return task;
}

/* -----------------------------------------------------------------------------
 * Kernel
 * ----------------------------------------------------------------------------- */

typedef struct Kernel {
  TaskQueue *ready;
  Selector  *selector;
  int ntasks;
  PyObject  *task_factory;
  int last_task_id;
} Kernel;

Kernel *
new_Kernel(PyObject *task_factory) {
  Kernel *kernel;

  kernel = (Kernel *) malloc(sizeof(Kernel));
  if (!kernel) {
    return NULL;
  }
  kernel->ready = new_TaskQueue();
  kernel->selector = new_Selector();
  kernel->ntasks = 0;
  kernel->task_factory = task_factory;
  kernel->last_task_id = 1;
  Py_INCREF(kernel->task_factory);
  return kernel;
}

void
delete_Kernel(Kernel *kernel) {
  delete_TaskQueue(kernel->ready);
  delete_Selector(kernel->selector);
  Py_XDECREF(kernel->task_factory);
  free(kernel);
}

/* Put a task on the ready queue */
void
Kernel_schedule_ready(Kernel *kernel, CTask *task) {
  TaskQueue_push(kernel->ready, task);
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
  printf("kernel_new_task %x\n", task);
  return task;
}

/* -----------------------------------------------------------------------------
 * traps
 * ----------------------------------------------------------------------------- */

typedef int (*trapfunc)(Kernel *, CTask *, PyObject *);

static int _trap_io(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *fileobj;
  int op;
  int fd;
  PyObject *statename;

  /* printf("trap_io\n"); */

  if (!PyArg_ParseTuple(args, "OOiO", &trapnum, &fileobj, &op, &statename)) {
    return 0;
  }

  fd = PyObject_AsFileDescriptor(fileobj);
  if (fd == -1) {
    return 0;
  }

  if ((task->last_io_op != op) || (task->last_io_fd != fd)) {
    if (task->last_io_fd > 0) {
      Selector_unregister(kernel->selector, task->last_io_fd);
    }
    Selector_register(kernel->selector, op, fd, (void *) task);
  }
  task->last_io_op = -1;
  task->last_io_fd = -1;
  return 1;
}

static int _trap_spawn(Kernel *kernel, CTask *task, PyObject *args) {
  PyObject *trapnum;
  PyObject *coro;
  PyObject *daemon;
  CTask *child;
  printf("spawn\n");
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
  }
  return 1;
}

static int _trap_get_kernel(Kernel *kernel, CTask *task, PyObject *args) {
  printf("get_kernel\n");
  Kernel_schedule_ready(kernel, task);
  return 1;
}

static int _trap_get_current(Kernel *kernel, CTask *task, PyObject *args) {
  printf("get_current\n");
  task->next_value = (PyObject *) task;
  Py_INCREF(task->next_value);
  Kernel_schedule_ready(kernel, task);
  return 1;
}

static trapfunc trap_table[15] = {
  _trap_io, /* 0 - trap_io */
  NULL,    /* 1 - */
  NULL,    /* 2 - sleep */
  _trap_spawn,    /* 3 - spawn */
  NULL,    /* 4 - */
  _trap_join,    /* 5 - join */
  NULL,    /* 6 - */
  NULL,    /* 7 - */
  NULL,    /* 8 - */
  NULL,    /* 9 - */
  NULL,    /* 10 - */
  _trap_get_kernel,    /* 11 - get_kernel */
  _trap_get_current,   /* 12 - get_current */
  NULL,    /* 13 - */
  NULL,    /* 14 - */
};

/* Cleanup a task that's terminated */
static void
cleanup_task(Kernel *kernel, CTask *task) {
  CTask *child;

  task->terminated = 1;

  printf("cleanup_task %x\n", task);
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
poll_for_io(Kernel *kernel) {
  IOEvent ioevents[MAX_EVENT];
  int nev, n;
  CTask *task;

  nev = Selector_select(kernel->selector, ioevents, MAX_EVENT);
  for (n = 0; n < nev; n++) {
    /* Selector_unregister(kernel->selector, ioevents[n].fileno); */
    task = (CTask *) ioevents[n].data;
    Kernel_schedule_ready(kernel, task);
    task->last_io_op = ioevents[n].op;
    task->last_io_fd = ioevents[n].fileno;
  }
}

/* -----------------------------------------------------------------------------
 * Core Kernel Loop
 * ----------------------------------------------------------------------------- */

PyObject *
Kernel_run(PyObject *coro, PyObject *task_factory) {
  Kernel *kernel;
  CTask *task;
  PyObject *request;

  kernel = new_Kernel(task_factory);
  task = Kernel_new_task(kernel, coro, NULL);
  Kernel_schedule_ready(kernel, task);
  kernel->ntasks++;

  while (kernel->ntasks > 0) {
    task = Kernel_next_ready(kernel);
    if (!task) {
      poll_for_io(kernel);
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

      printf("REQUEST:");
      PyObject_Print(request, stdout, 0);
      printf("\n");

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
	  Selector_unregister(kernel->selector, task->last_io_fd);
	  task->last_io_op = -1;
	  task->last_io_fd = -1;
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
	printf("Task Done\n");
      } else {
	printf("Task Crash\n");
	task->next_exc = value;
	Py_INCREF(value);
	PyErr_PrintEx(0);
      }
      /* Clean up the terminated task */
      cleanup_task(kernel, task);
      PyErr_Clear();
    }
  }
  delete_Kernel(kernel);
  return Py_BuildValue("");
}

PyObject *
py_run(PyObject *self, PyObject *args) {
  PyObject *coro;
  PyObject *task_factory;
  PyObject *result;
  if (!PyArg_ParseTuple(args, "OO", &coro, &task_factory)) {
    return NULL;
  }
  result = Kernel_run(coro, task_factory);
  return result;
}

static PyMethodDef ColonelMethods[] = {
  { "run", py_run, METH_VARARGS, NULL },
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

  return m;
}

