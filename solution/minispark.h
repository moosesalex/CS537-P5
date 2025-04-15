#ifndef __minispark_h__
#define __minispark_h__

#include <pthread.h>

#define MAXDEPS (2)
#define TIME_DIFF_MICROS(start, end) \
  (((end.tv_sec - start.tv_sec) * 1000000L) + ((end.tv_nsec - start.tv_nsec) / 1000L))

typedef struct RDD RDD;

struct Node
{
  void *data;
  struct Node *next;
};

struct List
{
  struct Node *head;
  struct Node *tail;
  int size;
};

typedef struct
{
  struct timespec created;
  struct timespec scheduled;
  size_t duration; // in usec
  RDD *rdd;
  int pnum;
} TaskMetric;

typedef struct
{
  RDD *rdd;
  int pnum;
  TaskMetric *metric;
} Task;

// a task queue is a queue of tasks (lol)
// these get eaten by the threads in the threadpool
typedef struct
{
  Task *tasks; // should this be a linkedlist?
  int size;
} TaskQueue;

// threadpool has a queue of tasks, and a queue of threads
// an available thread should get matched with the next thread
// when done, the thread rejoins the threads queue
typedef struct
{
  TaskQueue queue;
  pthread_t *threads;
  pthread_mutex_t queue_mutex;
} ThreadPool;

typedef struct RDD RDD;   // forward decl. of struct RDD
typedef struct List List; // forward decl. of List.
typedef struct Node Node; // forward decl. of Node.
// Minimally, we assume "list_add_elem(List *l, void*)"

// Different function pointer types used by minispark
typedef void *(*Mapper)(void *arg);
typedef int (*Filter)(void *arg, void *pred);
typedef void *(*Joiner)(void *arg1, void *arg2, void *arg);
typedef unsigned long (*Partitioner)(void *arg, int numpartitions, void *ctx);
typedef void (*Printer)(void *arg);

typedef enum
{
  MAP,
  FILTER,
  JOIN,
  PARTITIONBY,
  FILE_BACKED
} Transform;

struct RDD
{
  Transform trans;  // transform type, see enum
  void *fn;         // transformation function
  void *ctx;        // used by minispark lib functions
  List *partitions; // list of partitions

  RDD *dependencies[MAXDEPS];
  int numdependencies; // 0, 1, or 2
  int finisheddependencies;
  int numpartitions;
  // you may want extra data members here
  RDD *backlink; // pointer to the dependendent RDD. the backlink's numdependencies should be reduced by 1 when the current RDD is done
};

//////// actions ////////

// initializes a list
List *list_init(int size);

// appends data to the passed list
void list_add_elem(List *list, void *data);

// Return the total number of elements in "dataset"
int count(RDD *dataset);

// Print each element in "dataset" using "p".
// For example, p(element) for all elements.
void print(RDD *dataset, Printer p);

//////// transformations ////////

// Create an RDD with "rdd" as its dependency and "fn"
// as its transformation.
RDD *map(RDD *rdd, Mapper fn);

// Create an RDD with "rdd" as its dependency and "fn"
// as its transformation. "ctx" should be passed to "fn"
// when it is called as a Filter
RDD *filter(RDD *rdd, Filter fn, void *ctx);

// Create an RDD with two dependencies, "rdd1" and "rdd2"
// "ctx" should be passed to "fn" when it is called as a
// Joiner.
RDD *join(RDD *rdd1, RDD *rdd2, Joiner fn, void *ctx);

// Create an RDD with "rdd" as a dependency. The new RDD
// will have "numpartitions" number of partitions, which
// may be different than its dependency. "ctx" should be
// passed to "fn" when it is called as a Partitioner.
RDD *partitionBy(RDD *rdd, Partitioner fn, int numpartitions, void *ctx);

// Create an RDD which opens a list of files, one per
// partition. The number of partitions in the RDD will be
// equivalent to "numfiles."
RDD *RDDFromFiles(char *filenames[], int numfiles);

//////// MiniSpark ////////
// Submits work to the thread pool to materialize "rdd".
void execute(RDD *rdd);

// Creates the thread pool and monitoring thread.
void MS_Run();

// Waits for work to be complete, destroys the thread pool, and frees
// all RDDs allocated during runtime.
void MS_TearDown();

#endif // __minispark_h__
