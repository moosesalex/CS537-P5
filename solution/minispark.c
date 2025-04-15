#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sched.h>
#include <unistd.h>
#include "minispark.h"

#define TASK_QUEUE_BUFFER 255

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

    //What is this for?
    //if (rdd->trans == MAP)
    //{
    //  result = count(rdd);
    //}
    //Get the function that the rdd wants to execute
    void* (*function)(void*) = (void* (*)(void*))rdd->fn;
    //Materializes the rdd
    switch(rdd->trans){
      case MAP:
        rdd->partitions = rdd->dependencies[0]->partitions;
        for(int i; i < rdd->numpartitions; i++){
          Node* current = rdd->partitions[i].head;
          while(current != NULL){
            void* newData = current->data;
            newData = function(newData);
          }
        }
        break;
      case FILTER:

        break;
      case JOIN:

        break;
      case PARTITIONBY:

        break;
      case FILE_BACKED:
        return;
        break;
    }
    //We don't want to edit numdependencies so I made a new variable to tracked finished ones
    rdd->backlink->finisheddependencies++;
  }
  else
  {
    printf("RDD has dependencies, executing them first.\n");
    for (int i = 0; i < rdd->numdependencies; i++){
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
  free(node);
  return data;
}

// pops the rear of the queue,
// returns the data, and increases the rear
// pointer by 1

// starts a thread
void *start_thread(void *arg)
{
}

void *consumer(void *arg)
{
  TaskQueue *queue = (TaskQueue *)arg;
  while (1)
  {
    // get next task from queue (should this be an array?)
    pthread_mutex_lock(&queue->queue_mutex);
    while (queue->tasks == 0)
    {
      // wait for a task to be added to the queue
      pthread_cond_wait(&queue->fill, &queue->queue_mutex);
    }
  }
  int tmp = start_thread(&queue->rear); // what is do_get?
  // signal empty condition variable
  pthread_cond_signal(&queue->empty);
  pthread_mutex_unlock(&queue->queue_mutex);
  // execute task
  // free task
  // add metrics to log
  // if task is done, remove it from the queue
  // if task is not done, add it back to the queue
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

void MS_Run()
{
  // initalize threadpool
  // needs number of cpu cores
  cpu_set_t set;
  CPU_ZERO(&set);

  // Task *task = malloc(sizeof(Task));
  
  int THREAD_NUMBERS = CPU_COUNT(&set);

  if(sched_getaffinity(0, sizeof(set), &set) == -1)
  {
    perror("sched_getaffinity");
    exit(1);
  }

  printf("number of cores available: %d\n", THREAD_NUMBERS);

  // create threadpool
  ThreadPool *pool = malloc(sizeof(ThreadPool));
  if(pool == NULL)
  {
    printf("error mallocing threadpool\n");
    exit(1);
  }
  // initialize threadpool
  pool->threads = malloc(sizeof(pthread_t) * THREAD_NUMBERS);
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

  // create threads
  for (int i = 0; i < THREAD_NUMBERS; i++)
  {
    // create thread
    if (pthread_create(&pool->threads[i], NULL, &start_thread, pool) != 0)
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

void MS_TearDown()
{

  // wait for threads to finish
  /*
  for (int i = 0; i < THREAD_NUMBERS; i++)
  {
    if (pthread_join(threads[i], NULL) != 0)
    {
      printf("error joining thread\n");
      exit(1);
    }
  }
  */

  // wait for monitor thread to finish
  /*
  if (pthread_join(monitor_thread, NULL) != 0)
  {
    printf("error joining monitor thread\n");
    exit(1);
  }
  */

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
