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
#include "nodes/relation.h"
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

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * foreign table.  This information is collected by treasuredataGetForeignRelSize.
 */
typedef struct TdFdwRelationInfo
{
#if PG_VERSION_NUM >= 100000
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;
#endif

	/* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
	List	   *remote_conds;
	List	   *local_conds;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* Cost and selectivity of local_conds. */
	QualCost	local_conds_cost;
	Selectivity local_conds_sel;

	/* Estimated size and cost for a scan with baserestrictinfo quals. */
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/* Options extracted from catalogs. */
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;

#if PG_VERSION_NUM >= 100000
	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;

	/* Upper relation information */
	UpperRelationKind stage;

	/* Grouping information */
	List	   *grouped_tlist;
#endif

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;

	/* Query engine type */
	QueryEngineType query_engine_type;
} TdFdwRelationInfo;

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
