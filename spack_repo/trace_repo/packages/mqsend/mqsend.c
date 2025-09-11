#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <mqueue.h>
#include <string.h>
int main(int argc, char *argv[]) {
  if (argc != 4) {
    perror("Usage: mqsend QUEUE_NAME MSG PRIO");
    return 1;
  }
  char * queue_name = argv[1];
  char * msg = argv[2];
  int priority = atoi(argv[3]);
  mqd_t mqd = mq_open(queue_name, O_RDWR);
  if (mqd < 0) {
    // TODO: Error handling
    fprintf(stderr, "Opening queue: %s failed with error: %d\n", queue_name, errno);
    return 1;
  }
  size_t msg_len = strlen(msg);
  int send_res = mq_send(mqd, msg, msg_len, 1);
  if (send_res != 0) {
    fprintf(stderr, "Message send failed with error: %d\n", errno);
    return 1;
  }
  return 0;
}
