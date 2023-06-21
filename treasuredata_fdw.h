/*-------------------------------------------------------------------------
 *
 * treasuredata_fdw.h
 *		  Foreign-data wrapper for Treasure Data
 *
 * Portions Copyright (c) 2016, Mitsunori Komatsu
 *
 * IDENTIFICATION
 *		  treasuredata_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TREASUREDATA_FDW_H
#define TREASUREDATA_FDW_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"

#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#else
#include "executor/tuptable.h"
#include "optimizer/appendinfo.h"
#endif

#include "utils/relcache.h"

typedef struct TdFdwOption
{
	char *endpoint;      /* endpoint */
	char *query_engine;  /* query_engine */
	char *apikey;        /* apikey */
	char *database;      /* database name */
	char *table;         /* target table name */
	char *query;         /* static query */
	char *query_download_dir; /* dir name to download a query result temporally. If it's null, stream processing */
	long import_file_size; /* threshold size to split import files */
	bool atomic_import;  /* flag of whether split imported files get visible atomically */
}   TdFdwOption;

/*
 * Query engine type
 */
typedef enum
{
	QUERY_ENGINE_HIVE,
	QUERY_ENGINE_PRESTO
} QueryEngineType;

extern void ExtractFdwOptions(ForeignTable *table, TdFdwOption *fdw_option);

/* in deparse.c */
extern void classifyConditions(PlannerInfo *root,
                               RelOptInfo *baserel,
                               List *input_conds,
                               List **remote_conds,
                               List **local_conds);
extern bool is_foreign_expr(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Expr *expr);
extern void deparseSelectSql(StringInfo buf,
                             PlannerInfo *root,
                             RelOptInfo *baserel,
                             Bitmapset *attrs_used,
                             List **retrieved_attrs,
                             QueryEngineType query_engine_type);
extern void appendWhereClause(StringInfo buf,
                              PlannerInfo *root,
                              RelOptInfo *baserel,
                              List *exprs,
                              bool is_first,
                              List **params,
                              QueryEngineType query_engine_type);
extern void deparseInsertSql(StringInfo buf, PlannerInfo *root,
                             Index rtindex, Relation rel,
                             List *targetAttrs, bool doNothing, List *returningList,
                             List **retrieved_attrs);
extern void deparseUpdateSql(StringInfo buf, PlannerInfo *root,
                             Index rtindex, Relation rel,
                             List *targetAttrs, List *returningList,
                             List **retrieved_attrs);
extern void deparseDeleteSql(StringInfo buf, PlannerInfo *root,
                             Index rtindex, Relation rel,
                             List *returningList,
                             List **retrieved_attrs);
extern void deparseAnalyzeSizeSql(StringInfo buf, Relation rel);
extern void deparseAnalyzeSql(StringInfo buf, Relation rel,
                              List **retrieved_attrs);
extern void deparseStringLiteral(StringInfo buf, const char *val);

#endif   /* TREASUREDATA_FDW_H */
