#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sched.h>
#include <unistd.h>
#include "minispark.h"

#define TASK_QUEUE_BUFFER 255

volatile int pool_kill = 0;

ThreadPool *pool;
// Working with metrics...
// Recording the current time in a `struct timespec`:
//    clock_gettime(CLOCK_MONOTONIC, &metric->created);
// Getting the elapsed time in microseconds between two timespecs:
//    duration = TIME_DIFF_MICROS(metric->created, metric->scheduled);
// Use `print_formatted_metric(...)` to write a metric to the logfile.
void print_formatted_metric(TaskMetric *metric, FILE *fp)
{
  fprintf(fp, "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
          metric->rdd, metric->pnum, metric->rdd->trans,
          metric->created.tv_sec, metric->created.tv_nsec / 1000,
          metric->scheduled.tv_sec, metric->scheduled.tv_nsec / 1000,
          metric->duration);
}

int max(int a, int b)
{
  return a > b ? a : b;
}

RDD *create_rdd(int numdeps, Transform t, void *fn, ...)
{
  RDD *rdd = malloc(sizeof(RDD));
  if (rdd == NULL)
  {
    printf("error mallocing new rdd\n");
    exit(1);
  }

  va_list args;
  va_start(args, fn);

  int maxpartitions = 0;
  for (int i = 0; i < numdeps; i++)
  {
    RDD *dep = va_arg(args, RDD *);
    rdd->dependencies[i] = dep;
    maxpartitions = max(maxpartitions, dep->partitions->size);
  }
  va_end(args);

  rdd->numdependencies = numdeps;
  rdd->trans = t;
  rdd->fn = fn;
  rdd->partitions = NULL;
  rdd->finisheddependencies = 0;

  return rdd;
}

/* RDD constructors */
RDD *map(RDD *dep, Mapper fn)
{
  return create_rdd(1, MAP, fn, dep);
}

RDD *filter(RDD *dep, Filter fn, void *ctx)
{
  RDD *rdd = create_rdd(1, FILTER, fn, dep);
  rdd->ctx = ctx;
  return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx)
{
  RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep);
  rdd->partitions = list_init(numpartitions);
  rdd->numpartitions = numpartitions;
  rdd->ctx = ctx;
  return rdd;
}

RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx)
{
  RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
  rdd->ctx = ctx;
  return rdd;
}

/* A special mapper */
void *identity(void *arg)
{
  return arg;
}

/* Special RDD constructor.
 * By convention, this is how we read from input files. */
RDD *RDDFromFiles(char **filenames, int numfiles)
{
  RDD *rdd = malloc(sizeof(RDD));
  rdd->partitions = list_init(numfiles);

  for (int i = 0; i < numfiles; i++)
  {
    FILE *fp = fopen(filenames[i], "r");
    if (fp == NULL)
    {
      perror("fopen");
      exit(1);
    }
    list_add_elem(rdd->partitions, fp);
  }

  rdd->numdependencies = 0;
  rdd->trans = MAP;
  rdd->fn = (void *)identity;
  return rdd;
}

List *populatePartition(Task *task)
{
  RDD *rdd = task->rdd;
  int pnum = task->pnum;
  List *partition;
  Node *current;
  switch (rdd->trans)
  {
  case MAP:

    Mapper mapper = (Mapper)rdd->fn;
    partition = malloc(sizeof(List));
    current = rdd->dependencies[0]->partitions[pnum].head;
    while (current != NULL)
    {
      void *newData = current->data;
      list_add_elem(partition, mapper(newData));
      current = current->next;
    }
    rdd->partitions[pnum] = *partition;

    break;
  case FILTER:

    Filter filter = (Filter)rdd->fn;
    partition = malloc(sizeof(List));
    current = rdd->dependencies[0]->partitions[pnum].head;
    while (current != NULL)
    {
      void *newData = current->data;
      if (filter(newData, rdd->ctx))
      {
        list_add_elem(partition, newData);
      }
      current = current->next;
    }
    rdd->partitions[pnum] = *partition;

    break;
  case JOIN:

    Joiner joiner = (Joiner)rdd->fn;
    partition = malloc(sizeof(List));
    List partition0 = rdd->dependencies[0]->partitions[pnum];
    List partition1 = rdd->dependencies[1]->partitions[pnum];
    Node *current0 = partition0.head;
    while (current0 != NULL)
    {
      Node *current1 = partition1.head;
      while (current1 != NULL)
      {
        void *join = joiner(current0->data, current1->data, rdd->ctx);
        if (join != NULL)
        {
          list_add_elem(partition, join);
        }
        current1 = current1->next;
      }
      current0 = current0->next;
    }
    rdd->partitions[pnum] = *partition;
    break;
  case PARTITIONBY:
    printf("File Backed RDD's shouldn't be executed here!\n");
    break;
  case FILE_BACKED:
    printf("File Backed RDD's shouldn't be executed here!\n");
    exit(1);
    break;
  }
  return NULL;
}

void execute(RDD *rdd)
{
  // TODO: this should check to make sure RDD has 0 dependencies
  // if it does, we can execute it
  // add partitions to threadpool taskqueue for parallelism
  // if not, iterate to execute it's dependencies
  if (rdd->numdependencies - rdd->finisheddependencies == 0)
  {
    printf("RDD has no dependencies, ready to execute.\n");
    // in every Transformation case, we should add all partitions to the threadqueue?

    // Materializes the rdd
    if (rdd->trans == PARTITIONBY)
    {
      Partitioner partitioner = (Partitioner)rdd->fn;
      List *partitions = malloc(sizeof(List) * rdd->numpartitions);
      for (int i = 0; i < rdd->dependencies[0]->numpartitions; i++)
      {
        Node *current = rdd->dependencies[0]->partitions[i].head;
        while (current != NULL)
        {
          unsigned long hash = partitioner(current->data, rdd->numpartitions, rdd->ctx);
          list_add_elem(&(partitions[hash]), current->data);
          current = current->next;
        }
      }
      rdd->partitions = partitions;
    }
    if (rdd->trans == FILE_BACKED)
    {
      printf("I don't think we're supposed to execute File Backed rdd's\n");
      return;
    }

    // We don't want to edit numdependencies so I made a new variable to tracked finished ones
    rdd->backlink->finisheddependencies++;
  }
  else
  {
    printf("RDD has dependencies, executing them first.\n");
    for (int i = 0; i < rdd->numdependencies; i++)
    {
      execute(rdd->dependencies[i]);
    }
  }
  printf("executing rdd %p\n", rdd);
  return;
}

void testexecute(RDD *rdd)
{
  printf("called successfully");
  printf("rdd %p\n", rdd);
  printf("returning");
  return;
}

// allocates a list
List *list_init()
{
  List *list = malloc(sizeof(List));
  if (list == NULL)
  {
    printf("error mallocing new list\n");
    exit(1);
  }
  list->head = NULL;
  list->tail = NULL;
  return list;
}

// adds to list
void list_add_elem(List *list, void *data)
{
  Node *node = malloc(sizeof(Node));
  if (node == NULL)
  {
    printf("error mallocing new node\n");
    exit(1);
  }
  node->next = NULL;
  node->data = data;

  if (list->head == NULL)
  {
    list->head = node;
    list->tail = node;
  }
  else
  {
    list->tail->next = node;
    list->tail = node;
  }
}

// pops the head of the list and returns the data (like FIFO queue)
void *list_pop(List *list)
{
  if (list->head == NULL)
  {
    return NULL;
  }
  Node *node = list->head;
  list->head = node->next;
  void *data = node->data;
  list->size--;
  free(node);
  return data;
}

void *consumer()
{
  while (!pool_kill)
  {
    // get next task from queue (should this be an array?)
    pthread_mutex_lock(&pool->taskqueue->queue_mutex);
    while (pool->taskqueue->size == 0)
    {
      // wait for a task to be added to the queue
      pthread_cond_wait(&pool->taskqueue->fill, &pool->taskqueue->queue_mutex);
    }
    // task found
    Task task = pool->taskqueue->tasks[pool->taskqueue->rear];
    // shorted taskqueue
    pool->taskqueue->rear = (pool->taskqueue->rear + 1) % pool->taskqueue->capacity;
    pool->taskqueue->size--;
    // execute task
    populatePartition(&task);
    // signal empty condition variable
    pthread_cond_signal(&pool->taskqueue->empty);
    pthread_mutex_unlock(&pool->taskqueue->queue_mutex);
  }
}

TaskQueue *task_queue_init()
{
  TaskQueue *taskqueue = malloc(sizeof(TaskQueue));
  if (taskqueue == NULL)
  {
    printf("error mallocing task queue\n");
    exit(1);
  }
  taskqueue->tasks = malloc(sizeof(Task) * TASK_QUEUE_BUFFER);
  if (taskqueue->tasks == NULL)
  {
    printf("error mallocing tasks array\n");
    exit(1);
  }
  taskqueue->size = 0;
  taskqueue->capacity = TASK_QUEUE_BUFFER;
  taskqueue->front = 0;
  taskqueue->rear = 0;
  if (pthread_mutex_init(&taskqueue->queue_mutex, NULL) != 0)
  {
    printf("error initializing queue_mutex\n");
    exit(1);
  }
  if (pthread_cond_init(&taskqueue->fill, NULL) != 0)
  {
    printf("error initializing fill condition variable\n");
    exit(1);
  }
  if (pthread_cond_init(&taskqueue->empty, NULL) != 0)
  {
    printf("error initializing empty condition variable\n");
    exit(1);
  }
  return taskqueue;
}

void task_queue_add(Task *task)
{
  pthread_mutex_lock(&pool->taskqueue->queue_mutex);
  while (pool->taskqueue->size == pool->taskqueue->capacity)
  {
    // wait for a task to be removed from the queue
    pthread_cond_wait(&pool->taskqueue->empty, &pool->taskqueue->queue_mutex);
  }
  // check if queue is full
  if (pool->taskqueue->size == pool->taskqueue->capacity)
  {
    printf("error adding task to queue, queue is full\n");
    exit(1);
  }
  // add task to queue
  pool->taskqueue->tasks[pool->taskqueue->front] = *task;
  pool->taskqueue->front = (pool->taskqueue->front + 1) % pool->taskqueue->capacity;
  pool->taskqueue->size++;
  pthread_cond_signal(&pool->taskqueue->fill);
  pthread_mutex_unlock(&pool->taskqueue->queue_mutex);
}

void MS_Run()
{
  // initalize threadpool
  // needs number of cpu cores
  cpu_set_t set;
  CPU_ZERO(&set);

  // Task *task = malloc(sizeof(Task));

  if (sched_getaffinity(0, sizeof(set), &set) == -1)
  {
    perror("sched_getaffinity");
    exit(1);
  }

  printf("number of cores available: %d\n", pool->numthreads);

  // create threadpool
  pool = malloc(sizeof(ThreadPool));
  if (pool == NULL)
  {
    printf("error mallocing threadpool\n");
    exit(1);
  }
  // initialize num threads
  pool->numthreads = CPU_COUNT(&set);
  ;
  // initialize threadpool
  pool->threads = malloc(sizeof(pthread_t) * pool->numthreads);
  if (pool->threads == NULL)
  {
    printf("error mallocing threadpool threads\n");
    exit(1);
  }
  // initialize task queue
  pool->taskqueue = task_queue_init();
  // initialize lock
  if (pthread_mutex_init(&pool->pool_mutex, NULL) != 0)
  {
    printf("error initializing pool_mutex\n");
    exit(1);
  }

  // create consumer threads
  for (int i = 0; i < pool->numthreads; i++)
  {
    // create thread
    if (pthread_create(&pool->threads[i], NULL, &consumer, NULL) != 0)
    {
      printf("error creating thread\n");
      exit(1);
    }
  }

  // create monitoring thread
  /*
  pthread_t monitor_thread;
  if (pthread_create(&monitor_thread, NULL, &monitor_start_thread, NULL) != 0)
  {
    printf("error creating monitor thread\n");
    exit(1);
  }
  */

  return;
}

int getNumThreads()
{
  return pool->numthreads;
}

void MS_TearDown()
{

  // wait for threads to finish
  pool_kill = 1;
  pthread_cond_broadcast(&pool->taskqueue->fill);

  for (int i = 0; i < pool->numthreads; i++)
  {
    if (pthread_join(pool->threads[i], NULL) != 0)
    {
      printf("error joining thread\n");
      exit(1);
    }
  }

  // wait for monitor thread to finish
  /*
  if (pthread_join(monitor_thread, NULL) != 0)
  {
    printf("error joining monitor thread\n");
    exit(1);
  }
  */

  // free threadpool
  free(pool->threads);
  free(pool->taskqueue->tasks);
  free(pool->taskqueue);
  pthread_mutex_destroy(&pool->taskqueue->queue_mutex);
  pthread_mutex_destroy(&pool->pool_mutex);
  pthread_cond_destroy(&pool->taskqueue->fill);
  pthread_cond_destroy(&pool->taskqueue->empty);
  free(pool);

  return;
}

int count(RDD *rdd)
{
  execute(rdd);

  int count = 0;
  Node *current = rdd->partitions->head;
  while (current)
  {
    count++;
    current = current->next;
  }
  return count;
}

void print(RDD *rdd, Printer p)
{
  execute(rdd);

  // print all the items in rdd
  // aka... `p(item)` for all items in rdd
  for (int i = 0; i < rdd->numpartitions; i++)
  {
    Node *current = rdd->partitions[i].head;
    while (current != NULL)
    {
      p(current->data);
      current = current->next;
    }
  }
}
