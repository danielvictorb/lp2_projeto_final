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
    printf("\n=== Estatisticas Locais ===\n");
    int any = 0;
    for (int i = 0; i < NUM_SENSORS; i++) {
        if (!stats[i].tracked) continue;
        any = 1;
        sensor_stats_t *s = &stats[i];
        double avg = (s->received > 0) ? s->sum_val / s->received : 0.0;
        double pct = (s->received + s->lost > 0)
            ? 100.0 * s->lost / (s->received + s->lost) : 0.0;
        printf("%-12s recebidos=%u, perdidos=%u (%.1f%%), "
               "min=%.2f, max=%.2f, media=%.2f\n",
               s->name, s->received, s->lost, pct,
               s->min_val, s->max_val, avg);
    }
    if (!any) {
        printf("  Nenhum dado de sensor recebido ainda.\n");
    }
    printf("\n");
}

/* ------------------------------------------------------------------ */
/*  TCP response display                                               */
/* ------------------------------------------------------------------ */

static int is_known_header(const char *line)
{
    return strncmp(line, "Sensor:", 7) == 0 ||
           strncmp(line, "Status:", 7) == 0 ||
           strncmp(line, "UdpTarget:", 10) == 0 ||
           strncmp(line, "Interval:", 9) == 0 ||
           strncmp(line, "Count:", 6) == 0 ||
           strncmp(line, "Error:", 6) == 0 ||
           strncmp(line, "Seq:", 4) == 0 ||
           strncmp(line, "Value:", 6) == 0;
}

static void display_response(const char *raw)
{
    char copy[BUF_SIZE * 2];
    strncpy(copy, raw, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';

    char *term = strstr(copy, "\r\n\r\n");
    if (term) *term = '\0';

    char *lines[64];
    int nlines = 0;
    char *p = copy;
    while (*p && nlines < 64) {
        lines[nlines++] = p;
        char *nl = strstr(p, "\r\n");
        if (nl) {
            *nl = '\0';
            p = nl + 2;
        } else {
            break;
        }
    }

    if (nlines == 0) return;

    printf("Resposta: %s", lines[0]);

    int header_end = 1;
    for (int i = 1; i < nlines; i++) {
        if (is_known_header(lines[i])) {
            printf(" | %s", lines[i]);
            header_end = i + 1;
        } else {
            break;
        }
    }
    printf("\n");

    for (int i = header_end; i < nlines; i++) {
        if (lines[i][0] != '\0')
            printf("  %s\n", lines[i]);
    }
}

/* ------------------------------------------------------------------ */
/*  CLI command translation                                            */
/* ------------------------------------------------------------------ */

static const char *strip_sensor_prefix(const char *arg)
{
    if (strncmp(arg, "/sensor/", 8) == 0)
        return arg + 8;
    if (strncmp(arg, "/sensors", 8) == 0)
        return NULL;
    return arg;
}

static int build_request(const char *line, char *out, size_t out_size,
                         int udp_port, int *do_quit)
{
    char cmd[32] = {0};
    char arg[64] = {0};
    char arg2[16] = {0};

    int nargs = sscanf(line, "%31s %63s %15s", cmd, arg, arg2);
    if (nargs < 1) return -1;

    if (strcasecmp(cmd, "stats") == 0) {
        print_stats();
        return 0;
    }
    if (strcasecmp(cmd, "quit") == 0 || strcasecmp(cmd, "exit") == 0) {
        int n = snprintf(out, out_size, "EXIT /" CRLF CRLF);
        *do_quit = 1;
        return n;
    }

    if (strcasecmp(cmd, "start") == 0) {
        if (nargs < 2) {
            printf("Uso: start <sensor> [porta_udp]\n");
            return 0;
        }
        const char *sensor_name = strip_sensor_prefix(arg);
        if (!sensor_name) {
            printf("Uso: start <sensor> [porta_udp]\n");
            return 0;
        }
        int port = udp_port;
        if (nargs >= 3) port = atoi(arg2);

        int n = snprintf(out, out_size,
                         "START /sensor/%s" CRLF
                         "UdpPort: %d" CRLF
                         CRLF,
                         sensor_name, port);
        return n;
    }

    if (strcasecmp(cmd, "stop") == 0) {
        if (nargs < 2) {
            printf("Uso: stop <sensor>\n");
            return 0;
        }
        const char *sensor_name = strip_sensor_prefix(arg);
        if (!sensor_name) {
            printf("Uso: stop <sensor>\n");
            return 0;
        }
        int n = snprintf(out, out_size,
                         "STOP /sensor/%s" CRLF
                         CRLF,
                         sensor_name);
        return n;
    }

    if (strcasecmp(cmd, "status") == 0) {
        int n;
        if (nargs >= 2) {
            const char *sensor_name = strip_sensor_prefix(arg);
            if (!sensor_name || strcmp(arg, "/sensors") == 0) {
                n = snprintf(out, out_size,
                             "STATUS /sensors" CRLF
                             CRLF);
            } else {
                n = snprintf(out, out_size,
                             "STATUS /sensor/%s" CRLF
                             CRLF,
                             sensor_name);
            }
        } else {
            n = snprintf(out, out_size,
                         "STATUS /sensors" CRLF
                         CRLF);
        }
        return n;
    }

    printf("Comando desconhecido: %s\n", cmd);
    printf("Comandos: start <sensor>, stop <sensor>, status [sensor], stats, quit\n");
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
    printf("Conectado ao servidor %s:%d\n", server_host, tcp_port);

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
    printf("Socket UDP escutando na porta %d\n", udp_port);
    printf("Digite comandos (START, STOP, STATUS, stats, quit):\n\n");

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

            if (strcasecmp(line, "help") == 0) {
                printf("Comandos disponiveis:\n");
                printf("  start <sensor> [porta_udp] — inicia streaming (temperatura, umidade, pressao)\n");
                printf("  stop <sensor>              — para o streaming\n");
                printf("  status [sensor]            — consulta estado dos sensores\n");
                printf("  stats                      — exibe estatisticas locais\n");
                printf("  quit                       — envia EXIT e encerra\n\n");
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
                printf("Enviando EXIT ao servidor...\n");
                usleep(200000);

                char tmp[BUF_SIZE];
                ssize_t nr = recv(tcp_fd, tmp, sizeof(tmp) - 1, MSG_DONTWAIT);
                if (nr > 0) {
                    tmp[nr] = '\0';
                    display_response(tmp);
                }

                printf("Conexao encerrada.\n");
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
                    printf("Servidor fechou a conexao.\n");
                else
                    perror("recv TCP");
                running = 0;
                break;
            }
            tcp_buf_len += (int)n;
            tcp_buf[tcp_buf_len] = '\0';

            char *end;
            while ((end = strstr(tcp_buf, MSG_END)) != NULL) {
                int msg_len = (int)(end - tcp_buf) + 4;
                char saved  = tcp_buf[msg_len];
                tcp_buf[msg_len] = '\0';

                display_response(tcp_buf);

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
                    printf("[UDP] SEQ:%-4u | %-12s | %.2f %-3s | TS:%s\n",
                           seq, sensor, value, unit, ts);
                    update_stats(sensor, seq, value);
                } else {
                    printf("[UDP] datagrama malformado: %s\n", dgram);
                }
            }
        }
    }

    close(tcp_fd);
    close(udp_fd);
    printf("Cliente encerrado.\n");
    return 0;
}
