/*-------------------------------------------------------------------------
 *
 * bridge.c
 *        Bridge between C and Rust
 *
 * Portions Copyright (c) 2016, Mitsunori Komatsu
 *
 * IDENTIFICATION
 *        bridge.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "bridge.h"

#ifndef WITHOUT_PG

#include <postgres.h>
#define LOG_LEVEL_DEBUG DEBUG1
#define LOG_LEVEL_WARNING WARNING
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
	char **values;
	int index;
} fetch_result_context;

static int add_nil(fetch_result_context *context);
static int add_bytes(fetch_result_context *context, size_t len, const char *s);
static List *plappend(List *list, size_t len, const char *s);
static void debug_log(size_t len, const char *msg);
static void warning_log(size_t len, const char *msg);
static void error_log(size_t len, const char *msg);

extern void *issue_query(
    const char *apikey,
    const char *endpoint,
    const char *query_engine,
    const char *database,
    const char *query,
    const char *query_download_dir,
    void (*debug_log)(size_t, const char*),
    void (*error_log)(size_t, const char*)
);

extern bool fetch_result_row(
    void *query_state,
    fetch_result_context *context,
    int (*add_nil)(fetch_result_context *),
    int (*add_bytes)(fetch_result_context *, size_t, const char *),
    void (*debug_log)(size_t, const char *),
    void (*error_log)(size_t, const char *)
);

extern void release_query_resource(void *td_query_state);

extern void create_table(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table,
    void (*debug_log)(size_t, const char*),
    void (*error_log)(size_t, const char*)
);

extern void copy_table_schema(
    const char *apikey,
    const char *endpoint,
    const char *src_database,
    const char *src_table,
    const char *dst_database,
    const char *dst_table,
    void (*debug_log)(size_t, const char*),
    void (*error_log)(size_t, const char*)
);

extern void append_table_schema(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table,
    int column_size,
    const char **coltypes,
    const char **colnames,
    void (*debug_log)(size_t, const char *),
    void (*error_log)(size_t, const char *)
);

extern void delete_table(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table,
    void (*debug_log)(size_t, const char*),
    void (*error_log)(size_t, const char*)
);

extern void *import_begin(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table,
    int column_size,
    const char **coltypes,
    const char **colnames,
    void (*debug_log)(size_t, const char*),
    void (*error_log)(size_t, const char*)
);

extern size_t import_append(
    void *import_state,
    const char **values,
    void (*debug_log)(size_t, const char *),
    void (*error_log)(size_t, const char *)
);

extern void import_commit(
    void *import_state,
    void (*debug_log)(size_t, const char *),
    void (*error_log)(size_t, const char *)
);

#ifndef WITHOUT_PG
extern List *import_schema(
	const char *apikey,
    const char *endpoint,
	const char *query_engine,
	const char *database,
	const char *server,
	List *commands,
    List *(*plappend)(List *, size_t, const char *),
    void (*debug_log)(size_t, const char *),
	void (*warning_log)(size_t, const char *),
    void (*error_log)(size_t, const char *)
);
#endif

void *issueQuery(
    const char *apikey,
    const char *endpoint,
    const char *query_engine,
    const char *database,
    const char *query,
    const char *query_download_dir)
{
	return issue_query(
	           apikey,
	           endpoint,
	           query_engine,
	           database,
	           query,
	           query_download_dir,
	           debug_log,
	           error_log);
}

int fetchResultRow(void *td_query_state, int natts, char **values)
{
	int ret;
	fetch_result_context context;

	context.index = 0;
	context.values = values;

	ret = fetch_result_row(
	          td_query_state,
	          &context,
	          add_nil,
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

void releaseQueryResource(void *td_query_state)
{
	release_query_resource(td_query_state);
}

void createTable(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table)
{
	create_table(
	    apikey,
	    endpoint,
	    database,
	    table,
	    debug_log,
	    error_log);
}

void copyTableSchema(
    const char *apikey,
    const char *endpoint,
    const char *src_database,
    const char *src_table,
    const char *dst_database,
    const char *dst_table)
{
	copy_table_schema(
	    apikey,
	    endpoint,
	    src_database,
	    src_table,
	    dst_database,
	    dst_table,
	    debug_log,
	    error_log);
}

void appendTableSchema(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table,
    int column_size,
    const char **coltypes,
    const char **colnames)
{
	append_table_schema(
	    apikey,
	    endpoint,
	    database,
	    table,
	    column_size,
	    coltypes,
	    colnames,
	    debug_log,
	    error_log);
}

void deleteTable(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table)
{
	delete_table(
	    apikey,
	    endpoint,
	    database,
	    table,
	    debug_log,
	    error_log);
}

void *importBegin(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table,
    int column_size,
    const char **coltypes,
    const char **colnames)
{
	return import_begin(
	           apikey,
	           endpoint,
	           database,
	           table,
	           column_size,
	           coltypes,
	           colnames,
	           debug_log,
	           error_log);
}

size_t importAppend(void *import_state, const char **values)
{
	return import_append(
	           import_state,
	           values,
	           debug_log,
	           error_log);
}

void importCommit(void *import_state)
{
	import_commit(
	    import_state,
	    debug_log,
	    error_log);
}

#ifndef WITHOUT_PG
List *importSchema(
	const char *apikey,
    const char *endpoint,
	const char *query_engine,
	const char *database,
	const char *server,
	List *commands)
{
	return import_schema(
		apikey,
		endpoint,
		query_engine,
		database,
		server,
		commands,
		plappend,
		debug_log,
		warning_log,
		error_log);
}

static List *plappend(List *list, size_t len, const char *s)
{
	char *buf = (char*)ALLOC(len + 1);
	memcpy(buf, s, len);
	buf[len] = '\0';
	ereport(DEBUG3,
			(errmsg("plappend: string length = %zu", len)));
	return lappend(list, buf);
}
#endif

static int add_nil(fetch_result_context *context)
{
	context->values[context->index] = NULL;
	context->index++;
	return 0;
}

static int add_bytes(fetch_result_context *context, size_t len, const char *s)
{
	char *buf = (char*)ALLOC(len + 1);
	memcpy(buf, s, len);
	buf[len] = '\0';
	context->values[context->index] = buf;
	context->index++;
	return 0;
}

static void call_elog(int mode, size_t len, const char *msg)
{
	char *buf = (char*)ALLOC(len + 1);
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
debug_log(size_t len, const char *msg)
{
	call_elog(LOG_LEVEL_DEBUG, len, msg);
}

static void
warning_log(size_t len, const char *msg)
{
	call_elog(LOG_LEVEL_WARNING, len, msg);
}

static void
error_log(size_t len, const char *msg)
{
	call_elog(LOG_LEVEL_ERROR, len, msg);
}
