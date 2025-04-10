#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sched.h>
#include <stdarg.h>
#include "lib.h"
#include "minispark.h"

int main(int argc, char* argv[]) {
  struct timeval start, end;

  cpu_set_t set;
  CPU_ZERO(&set);

  if (sched_getaffinity(0, sizeof(set), &set) == -1) {
    perror("sched_getaffinity");
    exit(1);
  }
  
  int cpu_cnt = CPU_COUNT(&set);
  char *filenames[1000];
  for (int i=0; i< 1000; i++) {
    char *buffer = calloc(20,1);
    sprintf(buffer, "./test_files/%d", i);
    filenames[i] = buffer;
  }

  MS_Run();
  RDD* files = RDDFromFiles(filenames, 1000);
  // Get starting time
  gettimeofday(&start, NULL);
  count(filter(map(files, GetLines), SleepSecFilter, NULL));
  // Call the function to measure

  // Get ending time
  gettimeofday(&end, NULL);

  MS_TearDown();
  // Calculate elapsed time in microseconds
  long seconds = end.tv_sec - start.tv_sec;
  long microseconds = end.tv_usec - start.tv_usec;
  double elapsed = seconds + microseconds * 1e-6;

  // Check if it scales. 
  double predict = (3* 10 / cpu_cnt)+1;
  if (elapsed < predict)
    printf("ok");
  else
    printf("Too slow. elapsed %.2f predict %.2f\n", elapsed, predict);

  for (int i=0; i< 1000; i++) {
    free(filenames[i]);
  }
  
  return 0;
}
