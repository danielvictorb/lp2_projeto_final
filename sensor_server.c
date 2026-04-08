/*
 * sensor_server.c — IoT sensor monitoring server.
 *
 * Listens on a TCP port for control commands (START, STOP, STATUS, EXIT)
 * and spawns per-sensor threads that stream simulated readings over UDP
 * back to the client.
 *
 * Concurrency model
 * -----------------
 *   The main thread handles the TCP control channel in a blocking loop
 *   with a receive buffer that accumulates partial reads and scans for
 *   the \r\n\r\n message terminator.
 *
 *   Each active sensor gets its own pthread that sleeps for the sensor's
 *   configured interval and sends one UDP datagram per tick.  A per-sensor
 *   `active` flag (volatile int, protected by a mutex only for the
 *   pthread_join coordination) lets the main thread signal a sensor
 *   thread to stop.
 *
 * Compilation:
 *   gcc -Wall -Wextra -pthread -o sensor_server sensor_server.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <signal.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocol.h"

/* ------------------------------------------------------------------ */
/*  Per-sensor runtime state                                           */
/* ------------------------------------------------------------------ */

typedef struct {
    volatile int    active;         /* 1 = streaming, 0 = idle          */
    pthread_t       thread;
    uint32_t        seq;            /* next sequence number (1-based)   */
    double          current_value;  /* last simulated reading           */

    /* UDP destination filled on START */
    struct sockaddr_in udp_dest;

    /* back-pointer into the catalogue */
    int             sensor_idx;

    /* the UDP socket fd shared across threads (read-only after init) */
    int             udp_fd;
} sensor_state_t;

static sensor_state_t sensors[NUM_SENSORS];

/* ------------------------------------------------------------------ */
/*  Logging helper                                                     */
/* ------------------------------------------------------------------ */

static void server_log(const char *fmt, ...)
{
    time_t     now  = time(NULL);
    struct tm *tm   = localtime(&now);
    char       ts[20];
    strftime(ts, sizeof(ts), "%H:%M:%S", tm);

    fprintf(stdout, "[%s] ", ts);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stdout, fmt, ap);
    va_end(ap);
    fprintf(stdout, "\n");
    fflush(stdout);
}

/* ------------------------------------------------------------------ */
/*  TCP response formatting                                            */
/* ------------------------------------------------------------------ */

/*
 * Send a complete protocol response over the TCP control socket.
 * The format mirrors the request style: status line, optional body,
 * terminated by \r\n\r\n.
 */
static void send_response(int tcp_fd, int code, const char *reason,
                           const char *body)
{
    char buf[BUF_SIZE];
    int  n;

    if (body && body[0] != '\0') {
        n = snprintf(buf, sizeof(buf),
                     "%d %s" CRLF
                     "Content-Length: %zu" CRLF
                     CRLF
                     "%s" CRLF,
                     code, reason,
                     strlen(body),
                     body);
    } else {
        n = snprintf(buf, sizeof(buf),
                     "%d %s" CRLF
                     CRLF,
                     code, reason);
    }

    if (n > 0) {
        ssize_t sent = send(tcp_fd, buf, (size_t)n, 0);
        if (sent < 0)
            perror("send response");
    }
}

/* ------------------------------------------------------------------ */
/*  Sensor simulation thread                                           */
/* ------------------------------------------------------------------ */

/*
 * Bounded random walk: each tick adds a small uniformly-distributed
 * delta (±0.5 for temperature/humidity, ±1.0 for pressure) and clamps
 * the result to the sensor's configured range.
 */
static double random_walk(double current, double min_v, double max_v,
                          double max_delta)
{
    double delta = ((double)rand() / RAND_MAX) * 2.0 * max_delta - max_delta;
    double next  = current + delta;
    if (next < min_v) next = min_v;
    if (next > max_v) next = max_v;
    return next;
}

static void *sensor_thread_fn(void *arg)
{
    sensor_state_t     *st  = (sensor_state_t *)arg;
    const sensor_def_t *def = &SENSOR_CATALOGUE[st->sensor_idx];
    int interval_us = def->interval_ms * 1000;

    /*
     * Choose a delta magnitude proportional to the sensor range so that
     * the walk looks natural regardless of scale.
     */
    double max_delta = (def->max_val - def->min_val) * 0.01;

    server_log("Sensor '%s' thread started (interval %d ms, UDP -> %s:%d)",
               def->name, def->interval_ms,
               inet_ntoa(st->udp_dest.sin_addr),
               ntohs(st->udp_dest.sin_port));

    while (st->active) {
        st->current_value = random_walk(st->current_value,
                                        def->min_val, def->max_val,
                                        max_delta);
        st->seq++;

        /* Build the UDP datagram exactly as specified */
        char dgram[BUF_SIZE];
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);

        int len = snprintf(dgram, sizeof(dgram),
                           "SEQ:%u|SENSOR:%s|VALUE:%.2f|UNIT:%s|TS:%ld.%03ld",
                           st->seq, def->name, st->current_value,
                           def->unit,
                           (long)ts.tv_sec, ts.tv_nsec / 1000000L);

        ssize_t sent = sendto(st->udp_fd, dgram, (size_t)len, 0,
                              (struct sockaddr *)&st->udp_dest,
                              sizeof(st->udp_dest));
        if (sent < 0 && st->active)
            perror("sendto UDP");

        usleep(interval_us);
    }

    server_log("Sensor '%s' thread stopped (seq reached %u)",
               def->name, st->seq);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Command handling                                                   */
/* ------------------------------------------------------------------ */

/*
 * Resolve a sensor name from a resource path like "/sensor/temperatura".
 * Returns the catalogue index, or -1 if the path is malformed or the
 * sensor name is not in the catalogue.
 */
static int parse_sensor_resource(const char *resource)
{
    if (strncmp(resource, "/sensor/", 8) != 0)
        return -1;
    const char *name = resource + 8;
    return sensor_index(name);
}

/*
 * Extract the value of a header from the raw request text.
 * Writes into `out` at most `out_size` bytes.  Returns 0 on success.
 */
static int extract_header(const char *request, const char *header_name,
                          char *out, size_t out_size)
{
    char needle[256];
    snprintf(needle, sizeof(needle), "%s:", header_name);

    const char *p = strstr(request, needle);
    if (!p) return -1;

    p += strlen(needle);
    while (*p == ' ' || *p == '\t') p++;   /* skip optional whitespace */

    size_t i = 0;
    while (*p && *p != '\r' && *p != '\n' && i < out_size - 1) {
        out[i++] = *p++;
    }
    out[i] = '\0';
    return 0;
}

static void handle_start(int tcp_fd, const char *resource,
                          const char *raw_request,
                          struct sockaddr_in *client_addr, int udp_fd)
{
    int idx = parse_sensor_resource(resource);
    if (idx < 0) {
        send_response(tcp_fd, STATUS_NOT_FOUND, "Not Found",
                      "Unknown sensor.");
        server_log("START rejected: unknown resource '%s'", resource);
        return;
    }

    if (sensors[idx].active) {
        send_response(tcp_fd, STATUS_CONFLICT, "Conflict",
                      "Sensor already active.");
        server_log("START rejected: '%s' already active",
                   SENSOR_CATALOGUE[idx].name);
        return;
    }

    /* UdpPort header is mandatory for START */
    char port_str[16];
    if (extract_header(raw_request, "UdpPort", port_str, sizeof(port_str)) != 0) {
        send_response(tcp_fd, STATUS_BAD_REQUEST, "Bad Request",
                      "Missing UdpPort header.");
        server_log("START rejected: missing UdpPort header");
        return;
    }
    int udp_port = atoi(port_str);
    if (udp_port <= 0 || udp_port > 65535) {
        send_response(tcp_fd, STATUS_BAD_REQUEST, "Bad Request",
                      "Invalid UdpPort value.");
        return;
    }

    /* Set up the sensor state and launch its thread */
    sensors[idx].sensor_idx = idx;
    sensors[idx].seq        = 0;
    sensors[idx].udp_fd     = udp_fd;
    sensors[idx].current_value =
        (SENSOR_CATALOGUE[idx].min_val + SENSOR_CATALOGUE[idx].max_val) / 2.0;

    sensors[idx].udp_dest.sin_family = AF_INET;
    sensors[idx].udp_dest.sin_addr   = client_addr->sin_addr;
    sensors[idx].udp_dest.sin_port   = htons((uint16_t)udp_port);

    sensors[idx].active = 1;

    if (pthread_create(&sensors[idx].thread, NULL,
                       sensor_thread_fn, &sensors[idx]) != 0) {
        sensors[idx].active = 0;
        send_response(tcp_fd, STATUS_BAD_REQUEST, "Bad Request",
                      "Failed to create sensor thread.");
        server_log("pthread_create failed for '%s'",
                   SENSOR_CATALOGUE[idx].name);
        return;
    }
    /* Detach so we don't have to join on normal stop */
    pthread_detach(sensors[idx].thread);

    server_log("START '%s' -> UDP %s:%d",
               SENSOR_CATALOGUE[idx].name,
               inet_ntoa(client_addr->sin_addr), udp_port);

    char body[128];
    snprintf(body, sizeof(body), "Sensor '%s' started.",
             SENSOR_CATALOGUE[idx].name);
    send_response(tcp_fd, STATUS_OK, "OK", body);
}

static void handle_stop(int tcp_fd, const char *resource)
{
    int idx = parse_sensor_resource(resource);
    if (idx < 0) {
        send_response(tcp_fd, STATUS_NOT_FOUND, "Not Found",
                      "Unknown sensor.");
        server_log("STOP rejected: unknown resource '%s'", resource);
        return;
    }

    if (!sensors[idx].active) {
        send_response(tcp_fd, STATUS_CONFLICT, "Conflict",
                      "Sensor already inactive.");
        server_log("STOP rejected: '%s' already inactive",
                   SENSOR_CATALOGUE[idx].name);
        return;
    }

    sensors[idx].active = 0;
    /* Give the thread time to notice and exit its loop */
    usleep(sensors[idx].udp_fd > 0 ?
           (unsigned)(SENSOR_CATALOGUE[idx].interval_ms * 1000 * 2) : 100000);

    server_log("STOP '%s'", SENSOR_CATALOGUE[idx].name);

    char body[128];
    snprintf(body, sizeof(body), "Sensor '%s' stopped.",
             SENSOR_CATALOGUE[idx].name);
    send_response(tcp_fd, STATUS_OK, "OK", body);
}

static void handle_status(int tcp_fd, const char *resource)
{
    /* STATUS /sensors — list all */
    if (strcmp(resource, "/sensors") == 0) {
        char body[BUF_SIZE];
        int  off = 0;
        for (int i = 0; i < NUM_SENSORS; i++) {
            off += snprintf(body + off, sizeof(body) - (size_t)off,
                            "%s: %s (seq %u)\n",
                            SENSOR_CATALOGUE[i].name,
                            sensors[i].active ? "active" : "inactive",
                            sensors[i].seq);
        }
        send_response(tcp_fd, STATUS_OK, "OK", body);
        server_log("STATUS /sensors");
        return;
    }

    /* STATUS /sensor/<tipo> — single sensor */
    int idx = parse_sensor_resource(resource);
    if (idx < 0) {
        send_response(tcp_fd, STATUS_NOT_FOUND, "Not Found",
                      "Unknown sensor.");
        server_log("STATUS rejected: unknown resource '%s'", resource);
        return;
    }

    char body[256];
    snprintf(body, sizeof(body),
             "%s: %s (seq %u, last value %.2f %s)\n",
             SENSOR_CATALOGUE[idx].name,
             sensors[idx].active ? "active" : "inactive",
             sensors[idx].seq,
             sensors[idx].current_value,
             SENSOR_CATALOGUE[idx].unit);
    send_response(tcp_fd, STATUS_OK, "OK", body);
    server_log("STATUS /sensor/%s", SENSOR_CATALOGUE[idx].name);
}

static void handle_exit(int tcp_fd)
{
    /* Stop every active sensor before replying */
    for (int i = 0; i < NUM_SENSORS; i++) {
        if (sensors[i].active) {
            sensors[i].active = 0;
        }
    }
    /* Brief pause to let threads finish their current iteration */
    usleep(600000);

    send_response(tcp_fd, STATUS_OK, "OK", "Server shutting down.");
    server_log("EXIT received — shutting down");
}

/* ------------------------------------------------------------------ */
/*  Request parser                                                     */
/* ------------------------------------------------------------------ */

/*
 * Parse one complete request (already terminated by \r\n\r\n).
 * Dispatch to the appropriate handler.
 *
 * Expected first line: METHOD resource\r\n
 * Then optional headers, then blank line.
 */
static void dispatch_request(int tcp_fd, const char *msg,
                              struct sockaddr_in *client_addr, int udp_fd,
                              int *running)
{
    char method[16]   = {0};
    char resource[128] = {0};

    /* Extract the request line */
    if (sscanf(msg, "%15s %127s", method, resource) < 2) {
        send_response(tcp_fd, STATUS_BAD_REQUEST, "Bad Request",
                      "Malformed request line.");
        server_log("Bad request: could not parse method/resource");
        return;
    }

    server_log("Received: %s %s", method, resource);

    if (strcmp(method, "START") == 0) {
        handle_start(tcp_fd, resource, msg, client_addr, udp_fd);
    } else if (strcmp(method, "STOP") == 0) {
        handle_stop(tcp_fd, resource);
    } else if (strcmp(method, "STATUS") == 0) {
        handle_status(tcp_fd, resource);
    } else if (strcmp(method, "EXIT") == 0) {
        handle_exit(tcp_fd);
        *running = 0;
    } else {
        send_response(tcp_fd, STATUS_BAD_REQUEST, "Bad Request",
                      "Unknown method.");
        server_log("Unknown method '%s'", method);
    }
}

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
    int tcp_port = DEFAULT_TCP_PORT;

    if (argc >= 2)
        tcp_port = atoi(argv[1]);

    /* Ignore SIGPIPE so a broken TCP connection doesn't kill us */
    signal(SIGPIPE, SIG_IGN);
    srand((unsigned)time(NULL));

    /* Initialize sensor states */
    memset(sensors, 0, sizeof(sensors));

    /* ---- Create TCP listening socket ---- */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket TCP"); exit(EXIT_FAILURE); }

    int optval = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR,
                   &optval, sizeof(optval)) < 0) {
        perror("setsockopt");
    }

    struct sockaddr_in srv_addr;
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family      = AF_INET;
    srv_addr.sin_addr.s_addr = INADDR_ANY;
    srv_addr.sin_port        = htons((uint16_t)tcp_port);

    if (bind(listen_fd, (struct sockaddr *)&srv_addr, sizeof(srv_addr)) < 0) {
        perror("bind TCP"); close(listen_fd); exit(EXIT_FAILURE);
    }

    if (listen(listen_fd, 1) < 0) {
        perror("listen"); close(listen_fd); exit(EXIT_FAILURE);
    }

    server_log("Listening on TCP port %d", tcp_port);

    /* ---- Create UDP socket for sending sensor data ---- */
    int udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_fd < 0) { perror("socket UDP"); close(listen_fd); exit(EXIT_FAILURE); }

    /* ---- Accept loop (one client at a time for this academic project) ---- */
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        server_log("Waiting for client connection...");
        int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr,
                               &addr_len);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        server_log("Client connected from %s:%d",
                   inet_ntoa(client_addr.sin_addr),
                   ntohs(client_addr.sin_port));

        /*
         * TCP receive buffer.  We accumulate bytes and scan for the
         * message terminator \r\n\r\n.  This handles the case where a
         * single recv returns only part of a message, or where multiple
         * messages arrive in one recv.
         */
        char recv_buf[BUF_SIZE * 2];
        int  buf_len = 0;
        int  running = 1;

        while (running) {
            ssize_t n = recv(client_fd, recv_buf + buf_len,
                             sizeof(recv_buf) - (size_t)buf_len - 1, 0);
            if (n <= 0) {
                if (n == 0)
                    server_log("Client disconnected");
                else
                    perror("recv");
                break;
            }
            buf_len += (int)n;
            recv_buf[buf_len] = '\0';

            /*
             * Process every complete message in the buffer.
             * A message ends at \r\n\r\n.
             */
            char *end;
            while ((end = strstr(recv_buf, MSG_END)) != NULL) {
                /* Length including the terminator */
                int msg_len = (int)(end - recv_buf) + 4;

                /* Null-terminate this message for the parser */
                char saved = recv_buf[msg_len];
                recv_buf[msg_len] = '\0';

                dispatch_request(client_fd, recv_buf,
                                 &client_addr, udp_fd, &running);

                recv_buf[msg_len] = saved;

                /* Shift remaining data to the front of the buffer */
                buf_len -= msg_len;
                memmove(recv_buf, recv_buf + msg_len, (size_t)buf_len);
                recv_buf[buf_len] = '\0';
            }

            /* Guard against buffer overflow from a client that never terminates */
            if (buf_len >= (int)(sizeof(recv_buf) - 1)) {
                server_log("Receive buffer overflow — clearing");
                buf_len = 0;
            }
        }

        /* Clean up: stop all sensors that may still be running */
        for (int i = 0; i < NUM_SENSORS; i++) {
            if (sensors[i].active) {
                sensors[i].active = 0;
            }
        }
        usleep(600000);

        close(client_fd);
        server_log("Client session ended");

        /* Reset sequence counters for the next session */
        for (int i = 0; i < NUM_SENSORS; i++) {
            sensors[i].seq = 0;
        }

        if (!running)
            break;     /* EXIT was received — shut down the server */
    }

    close(udp_fd);
    close(listen_fd);
    server_log("Server terminated.");
    return 0;
}
