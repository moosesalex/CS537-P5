#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sched.h>
#include <unistd.h>
#include "minispark.h"

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
  int result = -1;
  // TODO: this should check to make sure RDD has 0 dependencies
  // if it does, we can execute it
  // add partitions to threadpool taskqueue for parallelism
  // if not, iterate to execute it's dependencies
  if (rdd->numdependencies == 0)
  {
    printf("RDD has no dependencies, ready to execute.\n");
    // in every Transformation case, we should add all partitions to the threadqueue?
    if (rdd->trans == MAP)
    {
      result = count(rdd);
    }
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
  printf("result is %d\n", result);
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
List *list_init(int size)
{
  List *list = malloc(sizeof(List));
  if (list == NULL)
  {
    printf("error mallocing new list\n");
    exit(1);
  }
  list->size = size;
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

  printf("number of cores available: %d\n", CPU_COUNT(&set));

  return;
}

void MS_TearDown()
{
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
