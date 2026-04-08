CC      = gcc
CFLAGS  = -Wall -Wextra -pthread
TARGETS = sensor_server sensor_client

.PHONY: all server client clean

all: $(TARGETS)

server: sensor_server

client: sensor_client

sensor_server: sensor_server.c protocol.h
	$(CC) $(CFLAGS) -o $@ sensor_server.c

sensor_client: sensor_client.c protocol.h
	$(CC) $(CFLAGS) -o $@ sensor_client.c -lm

clean:
	rm -f $(TARGETS)
