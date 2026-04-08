/*
 * sensor_client.c — IoT sensor monitoring client.
 *
 * Connects to the sensor server over TCP for sending control commands
 * and receives simulated sensor data over UDP.  The client provides an
 * interactive CLI that lets the user start/stop sensors, query status,
 * view live UDP readings, and compute per-sensor statistics.
 *
 * Concurrency model
 * -----------------
 *   A single event loop driven by select() multiplexes three sources:
 *     1. stdin   — user commands
 *     2. TCP fd  — server responses
 *     3. UDP fd  — sensor datagrams
 *
 *   This avoids threads entirely on the client side, keeping state
 *   management straightforward.
 *
 * Compilation:
 *   gcc -Wall -Wextra -o sensor_client sensor_client.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <math.h>
#include <float.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "protocol.h"

/* ------------------------------------------------------------------ */
/*  Per-sensor statistics                                              */
/* ------------------------------------------------------------------ */

typedef struct {
    char     name[32];
    int      tracked;       /* 1 once the first packet for this sensor arrives */
    uint32_t received;
    uint32_t lost;
    uint32_t last_seq;      /* last sequence number seen                       */
    double   min_val;
    double   max_val;
    double   sum_val;
} sensor_stats_t;

static sensor_stats_t stats[NUM_SENSORS];

static void init_stats(void)
{
    for (int i = 0; i < NUM_SENSORS; i++) {
        strncpy(stats[i].name, SENSOR_CATALOGUE[i].name,
                sizeof(stats[i].name) - 1);
        stats[i].tracked  = 0;
        stats[i].received = 0;
        stats[i].lost     = 0;
        stats[i].last_seq = 0;
        stats[i].min_val  = DBL_MAX;
        stats[i].max_val  = -DBL_MAX;
        stats[i].sum_val  = 0.0;
    }
}

/* ------------------------------------------------------------------ */
/*  UDP datagram parser                                                */
/* ------------------------------------------------------------------ */

/*
 * Parse a datagram of the form:
 *   SEQ:<n>|SENSOR:<tipo>|VALUE:<valor>|UNIT:<unidade>|TS:<timestamp>
 *
 * Extracts each field into the provided output variables.
 * Returns 0 on success, -1 on format error.
 */
static int parse_datagram(const char *dgram,
                          uint32_t *seq, char *sensor, size_t sensor_sz,
                          double *value, char *unit, size_t unit_sz,
                          char *ts, size_t ts_sz)
{
    /*
     * We walk the string looking for each "KEY:value|" segment.
     * Using a tokenizer on '|' then checking the prefix is robust
     * enough and keeps the code readable.
     */
    char copy[BUF_SIZE];
    strncpy(copy, dgram, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';

    char *saveptr = NULL;
    int   fields  = 0;

    for (char *tok = strtok_r(copy, "|", &saveptr);
         tok != NULL;
         tok = strtok_r(NULL, "|", &saveptr))
    {
        if (strncmp(tok, "SEQ:", 4) == 0) {
            *seq = (uint32_t)strtoul(tok + 4, NULL, 10);
            fields++;
        } else if (strncmp(tok, "SENSOR:", 7) == 0) {
            strncpy(sensor, tok + 7, sensor_sz - 1);
            sensor[sensor_sz - 1] = '\0';
            fields++;
        } else if (strncmp(tok, "VALUE:", 6) == 0) {
            *value = strtod(tok + 6, NULL);
            fields++;
        } else if (strncmp(tok, "UNIT:", 5) == 0) {
            strncpy(unit, tok + 5, unit_sz - 1);
            unit[unit_sz - 1] = '\0';
            fields++;
        } else if (strncmp(tok, "TS:", 3) == 0) {
            strncpy(ts, tok + 3, ts_sz - 1);
            ts[ts_sz - 1] = '\0';
            fields++;
        }
    }

    return (fields == 5) ? 0 : -1;
}

/* ------------------------------------------------------------------ */
/*  Statistics update                                                  */
/* ------------------------------------------------------------------ */

static void update_stats(const char *sensor_name, uint32_t seq, double value)
{
    int idx = sensor_index(sensor_name);
    if (idx < 0) return;

    sensor_stats_t *s = &stats[idx];

    if (!s->tracked) {
        /* First packet for this sensor — initialise baseline */
        s->tracked  = 1;
        s->last_seq = seq;
        s->received = 1;
        s->lost     = 0;
        s->min_val  = value;
        s->max_val  = value;
        s->sum_val  = value;
        return;
    }

    s->received++;

    /*
     * Detect gaps: if seq jumped by more than 1, the difference minus 1
     * packets were lost.  We only count forward gaps; a duplicate or
     * out-of-order packet (seq <= last_seq) is ignored for loss counting.
     */
    if (seq > s->last_seq + 1) {
        s->lost += (seq - s->last_seq - 1);
    }
    if (seq > s->last_seq) {
        s->last_seq = seq;
    }

    if (value < s->min_val) s->min_val = value;
    if (value > s->max_val) s->max_val = value;
    s->sum_val += value;
}

static void print_stats(void)
{
    printf("\n===== Sensor Statistics =====\n");
    int any = 0;
    for (int i = 0; i < NUM_SENSORS; i++) {
        if (!stats[i].tracked) continue;
        any = 1;
        sensor_stats_t *s = &stats[i];
        double avg = (s->received > 0) ? s->sum_val / s->received : 0.0;
        printf("  [%s]\n", s->name);
        printf("    Received : %u\n", s->received);
        printf("    Lost     : %u\n", s->lost);
        printf("    Min      : %.2f\n", s->min_val);
        printf("    Max      : %.2f\n", s->max_val);
        printf("    Average  : %.2f\n", avg);
    }
    if (!any) {
        printf("  No sensor data received yet.\n");
    }
    printf("=============================\n\n");
}

/* ------------------------------------------------------------------ */
/*  CLI command translation                                            */
/* ------------------------------------------------------------------ */

/*
 * Build a protocol-level TCP request from a user-typed CLI line.
 * Returns the number of bytes written to `out`, or 0 if the line
 * is a local-only command (stats, quit) handled in-place, or -1
 * for an unrecognised command.
 *
 * Supported interactive commands:
 *   start <sensor> [udp_port]
 *   stop <sensor>
 *   status
 *   status <sensor>
 *   exit | quit
 *   stats
 */
static int build_request(const char *line, char *out, size_t out_size,
                         int udp_port, int *do_quit)
{
    char cmd[32] = {0};
    char arg[64] = {0};
    char arg2[16] = {0};

    int nargs = sscanf(line, "%31s %63s %15s", cmd, arg, arg2);
    if (nargs < 1) return -1;

    /* Local-only commands */
    if (strcasecmp(cmd, "stats") == 0) {
        print_stats();
        return 0;
    }
    if (strcasecmp(cmd, "quit") == 0) {
        int n = snprintf(out, out_size, "EXIT /" CRLF CRLF);
        *do_quit = 1;
        return n;
    }
    if (strcasecmp(cmd, "exit") == 0) {
        int n = snprintf(out, out_size, "EXIT /" CRLF CRLF);
        *do_quit = 1;
        return n;
    }

    /* Protocol commands */
    if (strcasecmp(cmd, "start") == 0) {
        if (nargs < 2) {
            printf("Usage: start <sensor> [udp_port]\n");
            return 0;
        }
        int port = udp_port;
        if (nargs >= 3) port = atoi(arg2);

        int n = snprintf(out, out_size,
                         "START /sensor/%s" CRLF
                         "UdpPort: %d" CRLF
                         CRLF,
                         arg, port);
        return n;
    }

    if (strcasecmp(cmd, "stop") == 0) {
        if (nargs < 2) {
            printf("Usage: stop <sensor>\n");
            return 0;
        }
        int n = snprintf(out, out_size,
                         "STOP /sensor/%s" CRLF
                         CRLF,
                         arg);
        return n;
    }

    if (strcasecmp(cmd, "status") == 0) {
        int n;
        if (nargs >= 2) {
            n = snprintf(out, out_size,
                         "STATUS /sensor/%s" CRLF
                         CRLF,
                         arg);
        } else {
            n = snprintf(out, out_size,
                         "STATUS /sensors" CRLF
                         CRLF);
        }
        return n;
    }

    printf("Unknown command: %s\n", cmd);
    printf("Commands: start <sensor> | stop <sensor> | status [sensor] | stats | quit\n");
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
    const char *server_host = "127.0.0.1";
    int tcp_port = DEFAULT_TCP_PORT;
    int udp_port = DEFAULT_UDP_PORT;

    if (argc >= 2) server_host = argv[1];
    if (argc >= 3) tcp_port = atoi(argv[2]);
    if (argc >= 4) udp_port = atoi(argv[3]);

    init_stats();

    /* ---- Resolve server address ---- */
    struct sockaddr_in srv_addr;
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_port   = htons((uint16_t)tcp_port);

    if (inet_pton(AF_INET, server_host, &srv_addr.sin_addr) <= 0) {
        struct hostent *he = gethostbyname(server_host);
        if (!he) {
            fprintf(stderr, "Cannot resolve host: %s\n", server_host);
            exit(EXIT_FAILURE);
        }
        memcpy(&srv_addr.sin_addr, he->h_addr_list[0], (size_t)he->h_length);
    }

    /* ---- TCP connection ---- */
    int tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_fd < 0) { perror("socket TCP"); exit(EXIT_FAILURE); }

    if (connect(tcp_fd, (struct sockaddr *)&srv_addr, sizeof(srv_addr)) < 0) {
        perror("connect");
        close(tcp_fd);
        exit(EXIT_FAILURE);
    }
    printf("Connected to server %s:%d (TCP)\n", server_host, tcp_port);

    /* ---- UDP receive socket ---- */
    int udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_fd < 0) { perror("socket UDP"); close(tcp_fd); exit(EXIT_FAILURE); }

    struct sockaddr_in udp_addr;
    memset(&udp_addr, 0, sizeof(udp_addr));
    udp_addr.sin_family      = AF_INET;
    udp_addr.sin_addr.s_addr = INADDR_ANY;
    udp_addr.sin_port        = htons((uint16_t)udp_port);

    if (bind(udp_fd, (struct sockaddr *)&udp_addr, sizeof(udp_addr)) < 0) {
        perror("bind UDP");
        close(tcp_fd);
        close(udp_fd);
        exit(EXIT_FAILURE);
    }
    printf("Listening for sensor data on UDP port %d\n", udp_port);
    printf("Type 'help' for available commands.\n\n");

    /* ---- TCP receive buffer for partial reads ---- */
    char tcp_buf[BUF_SIZE * 2];
    int  tcp_buf_len = 0;

    /* ---- select() event loop ---- */
    int running = 1;
    while (running) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(STDIN_FILENO, &rfds);
        FD_SET(tcp_fd, &rfds);
        FD_SET(udp_fd, &rfds);

        int max_fd = tcp_fd;
        if (udp_fd > max_fd) max_fd = udp_fd;

        /*
         * Short timeout so the prompt stays responsive even when UDP
         * data is flowing.  250 ms is a good middle ground.
         */
        struct timeval tv = { .tv_sec = 0, .tv_usec = 250000 };

        int ret = select(max_fd + 1, &rfds, NULL, NULL, &tv);
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        /* ---- stdin: user typed a command ---- */
        if (FD_ISSET(STDIN_FILENO, &rfds)) {
            char line[256];
            if (fgets(line, sizeof(line), stdin) == NULL) {
                running = 0;
                break;
            }
            /* Strip trailing newline */
            line[strcspn(line, "\n")] = '\0';
            if (line[0] == '\0') continue;

            /* Handle "help" locally */
            if (strcasecmp(line, "help") == 0) {
                printf("Commands:\n");
                printf("  start <sensor> [udp_port]  — start streaming (temperatura, umidade, pressao)\n");
                printf("  stop <sensor>              — stop streaming\n");
                printf("  status [sensor]            — query sensor status\n");
                printf("  stats                      — show local statistics\n");
                printf("  quit / exit                — disconnect and exit\n\n");
                continue;
            }

            char request[BUF_SIZE];
            int do_quit = 0;
            int n = build_request(line, request, sizeof(request),
                                  udp_port, &do_quit);
            if (n > 0) {
                ssize_t sent = send(tcp_fd, request, (size_t)n, 0);
                if (sent < 0) {
                    perror("send");
                    running = 0;
                    break;
                }
            }
            if (do_quit) {
                /*
                 * Give the server a moment to process EXIT and send its
                 * response before we start reading it.
                 */
                usleep(200000);

                /* Drain any pending TCP response */
                char tmp[BUF_SIZE];
                ssize_t nr = recv(tcp_fd, tmp, sizeof(tmp) - 1, MSG_DONTWAIT);
                if (nr > 0) {
                    tmp[nr] = '\0';
                    printf("\n--- Server Response ---\n%s---\n", tmp);
                }

                running = 0;
                break;
            }
        }

        /* ---- TCP: server response ---- */
        if (FD_ISSET(tcp_fd, &rfds)) {
            ssize_t n = recv(tcp_fd, tcp_buf + tcp_buf_len,
                             sizeof(tcp_buf) - (size_t)tcp_buf_len - 1, 0);
            if (n <= 0) {
                if (n == 0)
                    printf("Server closed the connection.\n");
                else
                    perror("recv TCP");
                running = 0;
                break;
            }
            tcp_buf_len += (int)n;
            tcp_buf[tcp_buf_len] = '\0';

            /*
             * Print each complete response (terminated by CRLF CRLF).
             * We keep any partial trailing data in the buffer.
             */
            char *end;
            while ((end = strstr(tcp_buf, MSG_END)) != NULL) {
                int msg_len = (int)(end - tcp_buf) + 4;
                char saved  = tcp_buf[msg_len];
                tcp_buf[msg_len] = '\0';

                printf("\n--- Server Response ---\n%s---\n", tcp_buf);

                tcp_buf[msg_len] = saved;
                tcp_buf_len -= msg_len;
                memmove(tcp_buf, tcp_buf + msg_len, (size_t)tcp_buf_len);
                tcp_buf[tcp_buf_len] = '\0';
            }
        }

        /* ---- UDP: sensor datagram ---- */
        if (FD_ISSET(udp_fd, &rfds)) {
            char dgram[BUF_SIZE];
            struct sockaddr_in from;
            socklen_t from_len = sizeof(from);

            ssize_t n = recvfrom(udp_fd, dgram, sizeof(dgram) - 1, 0,
                                 (struct sockaddr *)&from, &from_len);
            if (n > 0) {
                dgram[n] = '\0';

                uint32_t seq;
                char     sensor[32], unit[16], ts[32];
                double   value;

                if (parse_datagram(dgram, &seq, sensor, sizeof(sensor),
                                   &value, unit, sizeof(unit),
                                   ts, sizeof(ts)) == 0) {
                    printf("[UDP] SEQ:%u  %-12s  %.2f %s  (ts %s)\n",
                           seq, sensor, value, unit, ts);
                    update_stats(sensor, seq, value);
                } else {
                    printf("[UDP] malformed datagram: %s\n", dgram);
                }
            }
        }
    }

    close(tcp_fd);
    close(udp_fd);
    printf("Client terminated.\n");
    return 0;
}
