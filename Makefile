CC=/usr/bin/gcc
PWD=$(shell pwd)
posixmq: trace/posixmq.c
	$(CC) -lrt -shared -fPIC -o trace/posixmq.so trace/posixmq.c

install: posixmq
	spack config add "config:extensions:${PWD}"
