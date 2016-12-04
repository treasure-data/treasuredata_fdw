#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "bridge.h"

#ifndef WITHOUT_PG

#include <postgres.h>
#define LOG_LEVEL_DEBUG DEBUG1
#define LOG_LEVEL_ERROR ERROR
#define ALLOC(sz) (palloc(sz))
#define FREE(p) (pfree(p))

#else

#include <string.h>
#define bool char
#define LOG_LEVEL_DEBUG 0
#define LOG_LEVEL_ERROR 0
#define ALLOC(sz) (malloc(sz))
#define FREE(p) (free(p))

#endif

typedef struct
{
	char** values;
	int index;
} fetch_result_context;

static int add_nil(fetch_result_context* context);
static int add_bool(fetch_result_context* context, bool b);
static int add_u64(fetch_result_context* context, unsigned long i);
static int add_i64(fetch_result_context* context, signed long i);
static int add_f32(fetch_result_context* context, float f);
static int add_f64(fetch_result_context* context, double f);
static int add_string(fetch_result_context* context, size_t len, const char* s);
static int add_bytes(fetch_result_context* context, size_t len, const char* s);
static void debug_log(size_t len, const char* msg);
static void error_log(size_t len, const char* msg);

extern void* issue_query(
    const char* apikey,
    const char* endpoint,
    const char* query_engine,
    const char* database,
    const char* query,
    void (*debug_log)(size_t, const char*),
    void (*error_log)(size_t, const char*)
);

extern bool fetch_result_row(
    void* query_state,
    fetch_result_context* context,
    int (*add_nil)(fetch_result_context*),
    int (*add_bool)(fetch_result_context*, bool),
    int (*add_u64)(fetch_result_context*, unsigned long),
    int (*add_i64)(fetch_result_context*, signed long),
    int (*add_f32)(fetch_result_context*, float),
    int (*add_f64)(fetch_result_context*, double),
    int (*add_string)(fetch_result_context*, size_t, const char*),
    int (*add_bytes)(fetch_result_context*, size_t, const char*),
    void (*debug_log)(size_t, const char*),
    void (*error_log)(size_t, const char*)
);

void* issueQuery(
    const char* apikey,
    const char* endpoint,
    const char* query_engine,
    const char* database,
    const char* query)
{
	return issue_query(
	           apikey,
	           endpoint,
	           query_engine,
	           database,
	           query,
	           debug_log,
	           error_log);
}

int fetchResultRow(void* td_query_state, int natts, char** values)
{
	int ret;
	fetch_result_context context;

	context.index = 0;
	context.values = values;

	ret = fetch_result_row(
	          td_query_state,
	          &context,
	          add_nil,
	          add_bool,
	          add_u64,
	          add_i64,
	          add_f32,
	          add_f64,
	          add_string,
	          add_bytes,
	          debug_log,
	          error_log);

	/* If the return value is true, it means there are more results */
	if (ret)
	{
		return 0;
	}

	return 1;
}

static int add_nil(fetch_result_context* context)
{
	context->values[context->index] = NULL;
	context->index++;
	return 0;
}

static int add_bool(fetch_result_context* context, bool b)
{
	int buf_size = 2;
	void* buf = ALLOC(buf_size);
	snprintf(buf, buf_size, "%d", b ? 1 : 0);
	context->values[context->index] = buf;
	context->index++;
	return 0;
}

static int add_u64(fetch_result_context* context, unsigned long i)
{
	int buf_size = 24;
	void* buf = ALLOC(buf_size);
	snprintf(buf, buf_size, "%ld", i);
	context->values[context->index] = buf;
	context->index++;
	return 0;
}

static int add_i64(fetch_result_context* context, signed long i)
{
	int buf_size = 24;
	void* buf = ALLOC(buf_size);
	snprintf(buf, buf_size, "%ld", i);
	context->values[context->index] = buf;
	context->index++;
	return 0;
}

static int add_f32(fetch_result_context* context, float f)
{
	int buf_size = 24;
	void* buf = ALLOC(buf_size);
	snprintf(buf, buf_size, "%f", f);
	context->values[context->index] = buf;
	context->index++;
	return 0;
}

static int add_f64(fetch_result_context* context, double f)
{
	int buf_size = 24;
	void* buf = ALLOC(buf_size);
	snprintf(buf, buf_size, "%f", f);
	context->values[context->index] = buf;
	context->index++;
	return 0;
}

static int add_string(fetch_result_context* context, size_t len, const char* s)
{
	char* buf = (char*)ALLOC(len + 1);
	memcpy(buf, s, len);
	buf[len] = '\0';
	context->values[context->index] = buf;
	context->index++;
	return 0;
}

static int add_bytes(fetch_result_context* context, size_t len, const char* s)
{
	char* buf = (char*)ALLOC(len + 1);
	memcpy(buf, s, len);
	buf[len] = '\0';
	context->values[context->index] = buf;
	context->index++;
	return 0;
}

static void call_elog(int mode, size_t len, const char* msg)
{
	char* buf = (char*)ALLOC(len + 1);
	memcpy(buf, msg, len);
	buf[len] = '\0';
#ifndef WITHOUT_PG
	elog(mode, "%s", buf);
#else
	printf("%s\n", buf);
#endif

	FREE(buf);
}

static void
debug_log(size_t len, const char* msg)
{
	call_elog(LOG_LEVEL_DEBUG, len, msg);
}

static void
error_log(size_t len, const char* msg)
{
	call_elog(LOG_LEVEL_ERROR, len, msg);
}

