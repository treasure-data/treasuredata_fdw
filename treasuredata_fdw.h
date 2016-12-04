#include "postgres.h"
#include "access/relscan.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#ifndef PG_TREASUREDATA_FDW_H
#define PG_TREASUREDATA_FDW_H

/* Data structures */

typedef struct ConversionInfo
{
	char	   *attrname;
	FmgrInfo   *attinfunc;
	FmgrInfo   *attoutfunc;
	Oid			atttypoid;
	Oid			attioparam;
	int32		atttypmod;
	int			attnum;
	bool		is_array;
	int			attndims;
	bool		need_quote;
} ConversionInfo;

typedef struct TdFdwOption
{
	char *endpoint;      /* endpoint */
	char *query_engine;  /* query_engine */
	char *apikey;        /* apikey */
	char *database;      /* database name */
	char *table;         /* target table name */
	char *query;         /* pre-defined query */
}   TdFdwOption;

typedef struct TdFdwPlanState
{
	Oid			foreigntableid;
	AttrNumber	numattrs;
	List	   *target_list;
	List	   *qual_list;
	int			startupCost;
	ConversionInfo **cinfos;
	List	   *pathkeys; /* list of TdFdwDeparsedSortGroup) */
}	TdFdwPlanState;

typedef struct TdFdwExecState
{
	void       *td_client;
	/* Information carried from the plan phase. */
	Oid			foreigntableid;
	List	   *target_list;
	List	   *qual_list;
	Datum	   *values;
	bool	   *nulls;
	ConversionInfo **cinfos;
	int        *scaned_column_indexes;
	/* Common buffer to avoid repeated allocations */
	StringInfo	buffer;
	AttrNumber	rowidAttno;
	char	   *rowidAttrName;
	List	   *pathkeys; /* list of TdFdwDeparsedSortGroup) */
}	TdFdwExecState;

typedef struct TdFdwBaseQual
{
	AttrNumber	varattno;
	NodeTag		right_type;
	Oid			typeoid;
	char	   *opname;
	bool		isArray;
	bool		useOr;
}	TdFdwBaseQual;

typedef struct TdFdwConstQual
{
	TdFdwBaseQual base;
	Datum		value;
	bool		isnull;
}	TdFdwConstQual;

typedef struct TdFdwVarQual
{
	TdFdwBaseQual base;
	AttrNumber	rightvarattno;
}	TdFdwVarQual;

typedef struct TdFdwParamQual
{
	TdFdwBaseQual base;
	Expr	   *expr;
}	TdFdwParamQual;

typedef struct TdFdwDeparsedSortGroup
{
	Name 			attname;
	int				attnum;
	bool			reversed;
	bool			nulls_first;
	Name			collate;
	PathKey	*key;
} TdFdwDeparsedSortGroup;

/* Hash table mapping oid to fdw instances */
extern PGDLLIMPORT HTAB *InstancesHash;


/* query.c */
extern void extractRestrictions(Relids base_relids, Expr *node, List **quals);
extern List	*extractColumns(List *reltargetlist, List *restrictinfolist);
extern void initConversioninfo(ConversionInfo ** cinfo,
                               AttInMetadata *attinmeta);

extern Value *colnameFromVar(Var *var, PlannerInfo *root,
                             TdFdwPlanState * state);

extern void computeDeparsedSortGroup(List *deparsed, TdFdwPlanState *planstate,
                                     List **apply_pathkeys, List **deparsed_pathkeys);

extern List	*findPaths(PlannerInfo *root, RelOptInfo *baserel, List *possiblePaths,
                       int startupCost, TdFdwPlanState *state,
                       List *apply_pathkeys, List *deparsed_pathkeys);

extern List *deparse_sortgroup(PlannerInfo *root, Oid foreigntableid, RelOptInfo *rel);

extern void *datumToPython(Datum node, Oid typeoid, ConversionInfo * cinfo);

extern List	*serializeDeparsedSortGroup(List *pathkeys);
extern List	*deserializeDeparsedSortGroup(List *items);

#endif   /* PG_TREASUREDATA_FDW_H */

