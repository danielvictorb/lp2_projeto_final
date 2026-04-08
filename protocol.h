/*
 * protocol.h — Shared definitions for the IoT sensor monitoring system.
 *
 * Both sensor_server and sensor_client include this header so that port
 * defaults, buffer sizes, sensor metadata, and protocol constants stay
 * in sync across the two programs.
 */

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/*  Network defaults                                                   */
/* ------------------------------------------------------------------ */

#define DEFAULT_TCP_PORT  9000
#define DEFAULT_UDP_PORT  9001
#define BUF_SIZE          4096

/* ------------------------------------------------------------------ */
/*  Protocol framing                                                   */
/* ------------------------------------------------------------------ */

#define CRLF      "\r\n"
#define MSG_END   "\r\n\r\n"       /* marks the end of every TCP message */

/* ------------------------------------------------------------------ */
/*  Response status codes                                              */
/* ------------------------------------------------------------------ */

#define STATUS_OK           200
#define STATUS_BAD_REQUEST  400
#define STATUS_NOT_FOUND    404
#define STATUS_CONFLICT     409

/* ------------------------------------------------------------------ */
/*  Sensor catalogue                                                   */
/* ------------------------------------------------------------------ */

#define NUM_SENSORS 3

typedef struct {
    const char *name;       /* "temperatura", "umidade", "pressao"      */
    const char *unit;       /* "C", "%", "hPa"                         */
    double      min_val;    /* lower bound of simulated range           */
    double      max_val;    /* upper bound of simulated range           */
    int         interval_ms;/* sending period in milliseconds           */
} sensor_def_t;

/*
 * The catalogue is defined as a file-scope array so every translation
 * unit that includes this header gets its own copy.  Since there are
 * only three entries and the project has exactly two translation units,
 * this is the simplest approach without adding a .c file just for it.
 */
static const sensor_def_t SENSOR_CATALOGUE[NUM_SENSORS] = {
    { "temperatura", "C",   15.0,  40.0,  100 },
    { "umidade",     "%",   30.0,  90.0,  200 },
    { "pressao",     "hPa", 990.0, 1030.0, 500 },
};

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

/*
 * Return the catalogue index for a sensor name, or -1 if not found.
 */
static inline int sensor_index(const char *name)
{
    for (int i = 0; i < NUM_SENSORS; i++) {
        if (strcmp(name, SENSOR_CATALOGUE[i].name) == 0)
            return i;
    }
    return -1;
}

#endif /* PROTOCOL_H */
