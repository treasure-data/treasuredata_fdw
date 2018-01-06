/*-------------------------------------------------------------------------
 *
 * bridge.h
 *        Bridge between C and Rust
 *
 * Portions Copyright (c) 2016, Mitsunori Komatsu
 *
 * IDENTIFICATION
 *        bridge.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TREASUREDATA_FDW_BRIDGE_H
#define TREASUREDATA_FDW_BRIDGE_H

#ifndef WITHOUT_PG
#include "c.h"
#include "nodes/pg_list.h"
#endif

extern void* issueQuery(
    const char *apikey,
    const char *endpoint,
    const char *query_engine,
    const char *database,
    const char *query,
    const char *query_download_dir
);

extern int fetchResultRow(
    void *td_query_state, int natts, char **values
);

extern void releaseQueryResource(void *td_query_state);

extern void createTable(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table);

extern void copyTableSchema(
    const char *apikey,
    const char *endpoint,
    const char *src_database,
    const char *src_table,
    const char *dst_database,
    const char *dst_table);

extern void appendTableSchema(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table,
    int column_size,
    const char **coltypes,
    const char **colnames);

extern void deleteTable(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table);

extern void *importBegin(
    const char *apikey,
    const char *endpoint,
    const char *database,
    const char *table,
    int column_size,
    const char **coltypes,
    const char **colnames);

extern size_t importAppend(void *import_state, const char **values);

extern void importCommit(void *import_state);

#ifndef WITHOUT_PG
List *importSchema(
	const char *apikey,
    const char *endpoint,
	const char *query_engine,
	const char *database,
	const char *server,
	List *commands);
#endif

#endif   /* TREASUREDATA_FDW_BRIDGE_H */

