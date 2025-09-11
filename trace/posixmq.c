#include <mqueue.h>
#include <stddef.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#define RES_T int
RES_T posixmq_open_create(const char * name, const size_t max_msg_size,
                          const size_t max_queue_size, int * mq_id_out) {
  struct mq_attr attrs = {.mq_maxmsg = max_queue_size, .mq_msgsize = max_msg_size};
  mqd_t mqd = mq_open(name, O_CREAT | O_RDWR, 0644, &attrs);
  if (mqd < 0){
    // TODO: error handling
    printf("Errno: %d\n", errno);
    return -1;
  }
  *mq_id_out = mqd;
  return 0;
}

RES_T posixmq_open_existing(const char * name, int * mq_id_out,
                            size_t * const max_msg_size_out,
                            size_t * const max_queue_size_out){
  mqd_t mqd = mq_open(name, O_RDWR);
  if (mqd < 0) {
    // TODO: error handling
    printf("Errno: %d\n", errno);
    return -1;
  }
  struct mq_attr attrs;
  mq_getattr(mqd, &attrs);
  *mq_id_out = mqd;
  *max_queue_size_out = attrs.mq_maxmsg;
  *max_msg_size_out = attrs.mq_msgsize;
  return 0;
}

RES_T posixmq_close(const int mq) {
  if (mq_close(mq) < 0) {
    // TODO: Error handling
    printf("Errno: %d\n", errno);
    return -1;
  }
  return 0;
}

RES_T posixmq_unlink(const char* name) {
  if (mq_unlink(name) < 0) {
    // TODO: Error handling
    printf("Errno: %d\n", errno);
    return -1;
  }
  return 0;
  
}

RES_T posixmq_send(const int mq, char * const msg, const size_t size,
                   const unsigned int priority){
  if (mq_send(mq, msg, size, priority) < 0) {
    // TODO: Error Handling
    printf("Errno: %d\n", errno);
    return -1;
  }
  return 0;
}

RES_T posixmq_recv(const int mq, char* const buf_out, size_t * const size_inout,
                   unsigned int * const priority_out) {
  size_t res = mq_receive(mq, buf_out, *size_inout, priority_out);
  if (res < 0) {
    // TODO: Error Handling
    printf("Errno: %d\n", errno);
    return -1;
  }
  *size_inout = res;
  return 0;
}
