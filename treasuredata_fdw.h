/*-------------------------------------------------------------------------
 *
 * postgres_fdw.h
 *		  Foreign-data wrapper for remote PostgreSQL servers
 *
 * Portions Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/postgres_fdw/postgres_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POSTGRES_FDW_H
#define POSTGRES_FDW_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/relcache.h"

#include "libpq-fe.h"

typedef struct TdFdwOption
{
    char *endpoint;      /* endpoint */
    char *query_engine;  /* query_engine */
    char *apikey;        /* apikey */
    char *database;      /* database name */
    char *table;         /* target table name */
}   TdFdwOption;

#if 0
/* in postgres_fdw.c */
extern int	set_transmission_modes(void);
extern void reset_transmission_modes(int nestlevel);

/* in connection.c */
extern PGconn *GetConnection(ForeignServer *server, UserMapping *user,
			  bool will_prep_stmt);
extern void ReleaseConnection(PGconn *conn);
extern unsigned int GetCursorNumber(PGconn *conn);
extern unsigned int GetPrepStmtNumber(PGconn *conn);
extern void pgfdw_report_error(int elevel, PGresult *res, PGconn *conn,
				   bool clear, const char *sql);

/* in option.c */
extern int ExtractConnectionOptions(List *defelems,
						 const char **keywords,
						 const char **values);
#endif
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
				 List **retrieved_attrs);
extern void appendWhereClause(StringInfo buf,
				  PlannerInfo *root,
				  RelOptInfo *baserel,
				  List *exprs,
				  bool is_first,
				  List **params);
#if 0
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
#endif
extern void deparseStringLiteral(StringInfo buf, const char *val);

#endif   /* POSTGRES_FDW_H */
