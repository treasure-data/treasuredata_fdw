/*-------------------------------------------------------------------------
 *
 * treasuredata_fdw.c
 *		  Foreign-data wrapper for Treasure Data
 *
 * Portions Copyright (c) 2016, Mitsunori Komatsu
 *
 * IDENTIFICATION
 *		  treasuredata_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include <unistd.h>
#include <time.h>

#include "postgres.h"

#include "treasuredata_fdw.h"
#include "bridge.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

extern Datum treasuredata_fdw_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(treasuredata_fdw_handler);

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST		0.01

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * foreign table.  This information is collected by treasuredataGetForeignRelSize.
 */
typedef struct TdFdwRelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;

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

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;

	/* Upper relation information */
	UpperRelationKind stage;

	/* Grouping information */
	List	   *grouped_tlist;

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;

	/* Query engine type */
	QueryEngineType query_engine_type;
} TdFdwRelationInfo;

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * We store various information in ForeignScan.fdw_private to pass it from
 * planner to executor.  Currently we store:
 *
 * 1) SELECT statement text to be sent to the remote server
 * 2) Integer list of attribute numbers retrieved by the SELECT
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ModifyTable node referencing a treasuredata_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *	  (NIL for a DELETE)
 * 3) Boolean flag showing if the remote query has a RETURNING clause
 * 4) Integer list of attribute numbers retrieved by RETURNING, if any
 */
enum FdwModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
	/* has-returning flag (as an integer Value node) */
	FdwModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwModifyPrivateRetrievedAttrs
};

/*
 * Execution state of a foreign scan using treasuredata_fdw.
 */
typedef struct TdFdwScanState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char	   *query;			/* text of SELECT command */
	List	   *retrieved_attrs;	/* list of retrieved attribute numbers */
	bool        fetch_all_column;    /* If all columns always need to be fetched (in case of 'query' option mode) */

	/* for remote query execution */
	void       *td_client;
	int			numParams;		/* number of parameters passed to query */
	FmgrInfo   *param_flinfo;	/* output conversion functions for them */
	List	   *param_exprs;	/* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */

	/* for storing result tuples */
	HeapTuple  *tuples;			/* array of currently-retrieved tuples */
	int			num_tuples;		/* # of tuples in array */
	int			next_tuple;		/* index of next one to return */

	/* working memory contexts */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */

	/* Cached catalog information. */
	ForeignTable *table;
} TdFdwScanState;

/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct TdFdwModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* for remote query execution */
	void       *td_client;
	TdFdwOption fdw_option;
	char       *tmp_table_name; /* used for atomic import */

	/* extracted fdw_private data */
	char	   *query;			/* text of INSERT/UPDATE/DELETE command */
	List	   *target_attrs;	/* list of target attribute numbers */

	/* attribute info */
	AttrNumber	ctidAttno;		/* attnum of input resjunk ctid column */
	int			p_nums;			/* number of parameters to transmit */
	FmgrInfo   *p_flinfo;		/* output conversion functions for them */
	const char **column_types;
	const char **column_names;

	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */

	/* Cached catalog information. */
	ForeignTable *table;
} TdFdwModifyState;

/*
 * Identify the attribute where data conversion fails.
 */
typedef struct ConversionLocation
{
	Relation	rel;			/* foreign table's relcache entry */
	AttrNumber	cur_attno;		/* attribute number being processed, or 0 */
} ConversionLocation;

/* Callback argument for ec_member_matches_foreign */
typedef struct
{
	Expr	   *current;		/* current expr, or NULL if not yet found */
	List	   *already_used;	/* expressions already dealt with */
} ec_member_foreign_arg;

/*
 * FDW callback routines
 */
static void treasuredataGetForeignRelSize(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid);
static void treasuredataGetForeignPaths(PlannerInfo *root,
                                        RelOptInfo *baserel,
                                        Oid foreigntableid);
static ForeignScan *treasuredataGetForeignPlan(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid,
        ForeignPath *best_path,
        List *tlist,
        List *scan_clauses
#if PG_VERSION_NUM >= 90500
        , Plan *outer_plan
#endif
                                              );
static void treasuredataBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *treasuredataIterateForeignScan(ForeignScanState *node);
static void treasuredataEndForeignScan(ForeignScanState *node);
static void treasuredataAddForeignUpdateTargets(Query *parsetree,
        RangeTblEntry *target_rte,
        Relation target_relation);
static List *treasuredataPlanForeignModify(PlannerInfo *root,
        ModifyTable *plan,
        Index resultRelation,
        int subplan_index);
static void treasuredataBeginForeignModify(ModifyTableState *mtstate,
        ResultRelInfo *resultRelInfo,
        List *fdw_private,
        int subplan_index,
        int eflags);
static TupleTableSlot *treasuredataExecForeignInsert(EState *estate,
        ResultRelInfo *resultRelInfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);
static void treasuredataEndForeignModify(EState *estate,
        ResultRelInfo *resultRelInfo);
#if PG_VERSION_NUM >= 90500
static List *treasuredataImportForeignSchema(ImportForeignSchemaStmt *stmt,
        Oid serverOid);
#endif
#if PG_VERSION_NUM >= 90600
static void treasuredataGetForeignUpperPaths(PlannerInfo *root,
        UpperRelationKind stage,
        RelOptInfo *input_rel,
        RelOptInfo *output_rel,
        void *extra);
#endif

/*
 * Helper functions
 */
static void estimate_path_cost_size(PlannerInfo *root,
                                    RelOptInfo *baserel,
                                    List *join_conds,
                                    double *p_rows, int *p_width,
                                    Cost *p_startup_cost, Cost *p_total_cost);

static const char **convert_prep_stmt_params(TdFdwModifyState *fmstate,
        ItemPointer tupleid,
        TupleTableSlot *slot);

static HeapTuple make_tuple_from_result_row(void *td_client,
        bool fetch_all_column,
        Relation rel,
        AttInMetadata *attinmeta,
        List *retrieved_attrs,
        MemoryContext temp_context);
static void conversion_error_callback(void *arg);
static void add_foreign_grouping_paths(PlannerInfo *root,
									   RelOptInfo *input_rel,
									   RelOptInfo *grouped_rel,
									   GroupPathExtraData *extra);
static void merge_fdw_options(TdFdwRelationInfo *fpinfo,
							  const TdFdwRelationInfo *fpinfo_o,
							  const TdFdwRelationInfo *fpinfo_i);
static bool foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
								Node *havingQual);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
treasuredata_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = treasuredataGetForeignRelSize;
	routine->GetForeignPaths = treasuredataGetForeignPaths;
	routine->GetForeignPlan = treasuredataGetForeignPlan;
	routine->BeginForeignScan = treasuredataBeginForeignScan;
	routine->IterateForeignScan = treasuredataIterateForeignScan;
	routine->EndForeignScan = treasuredataEndForeignScan;

	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = treasuredataAddForeignUpdateTargets;
	routine->PlanForeignModify = treasuredataPlanForeignModify;
	routine->BeginForeignModify = treasuredataBeginForeignModify;
	routine->ExecForeignInsert = treasuredataExecForeignInsert;
	routine->EndForeignModify = treasuredataEndForeignModify;
#if PG_VERSION_NUM >= 90500
	/* Support functions for IMPORT FOREIGN SCHEMA */
	routine->ImportForeignSchema = treasuredataImportForeignSchema;
#endif
#if PG_VERSION_NUM >= 90600
    /* Support functions for upper relation push-down */
    routine->GetForeignUpperPaths = treasuredataGetForeignUpperPaths;
#endif

	PG_RETURN_POINTER(routine);
}

/*
 * treasuredataGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
static void
treasuredataGetForeignRelSize(PlannerInfo *root,
                              RelOptInfo *baserel,
                              Oid foreigntableid)
{
	TdFdwRelationInfo *fpinfo;
	ListCell   *lc;

	/*
	 * We use TdFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = (TdFdwRelationInfo *) palloc0(sizeof(TdFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);
	{
		TdFdwOption fdw_option;
		ExtractFdwOptions(fpinfo->table, &fdw_option);
		if (strcmp(fdw_option.query_engine, "presto") == 0)
			fpinfo->query_engine_type = QUERY_ENGINE_PRESTO;
		else
			fpinfo->query_engine_type = QUERY_ENGINE_HIVE;
	}

	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	classifyConditions(root, baserel, baserel->baserestrictinfo,
	                   &fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;
#if PG_VERSION_NUM >= 90600
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
	               &fpinfo->attrs_used);
#else
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid,
	               &fpinfo->attrs_used);
#endif
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
		               &fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
	                          fpinfo->local_conds,
	                          baserel->relid,
	                          JOIN_INNER,
	                          NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Estimate using whatever statistics we have locally, in a way similar
	 * to ordinary tables.
	 *
	 * If the foreign table has never been ANALYZEd, it will have relpages
	 * and reltuples equal to zero, which most likely has nothing to do
	 * with reality.  We can't do a whole lot about that if we're not
	 * allowed to consult the remote server, but we can use a hack similar
	 * to plancat.c's treatment of empty relations: use a minimum size
	 * estimate of 10 pages, and divide by the column-datatype-based width
	 * estimate to get the corresponding number of tuples.
	 */
	if (baserel->pages == 0 && baserel->tuples == 0)
	{
		baserel->pages = 10;
		baserel->tuples =
#if PG_VERSION_NUM >= 90600
		    (10 * BLCKSZ) / (baserel->reltarget->width + MAXALIGN(SizeofHeapTupleHeader));
#elif PG_VERSION_NUM >= 90500
		    (10 * BLCKSZ) / (baserel->width + MAXALIGN(SizeofHeapTupleHeader));
#else
		    (10 * BLCKSZ) / (baserel->width + sizeof(HeapTupleHeaderData));
#endif
	}

	/* Estimate baserel size as best we can with local statistics. */
	set_baserel_size_estimates(root, baserel);

	/* Fill in basically-bogus cost estimates for use later. */
	estimate_path_cost_size(root, baserel, NIL,
	                        &fpinfo->rows, &fpinfo->width,
	                        &fpinfo->startup_cost, &fpinfo->total_cost);
}

/*
 * treasuredataGetForeignPaths
 *		Create possible scan paths for a scan on the foreign table
 */
static void
treasuredataGetForeignPaths(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Oid foreigntableid)
{
	TdFdwRelationInfo *fpinfo = (TdFdwRelationInfo *) baserel->fdw_private;
	ForeignPath *path;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
	path = create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
	                               NULL,      /* default pathtarget */
#endif
	                               fpinfo->rows,
	                               fpinfo->startup_cost,
	                               fpinfo->total_cost,
	                               NIL, /* no pathkeys */
	                               NULL,		/* no outer rel either */
#if PG_VERSION_NUM >= 90500
	                               NULL,		/* no extra plan */
#endif
	                               NIL);		/* no fdw_private list */
	add_path(baserel, (Path *) path);
}

/*
 * treasuredataGetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
static ForeignScan *
treasuredataGetForeignPlan(PlannerInfo *root,
                           RelOptInfo *baserel,
                           Oid foreigntableid,
                           ForeignPath *best_path,
                           List *tlist,
                           List *scan_clauses
#if PG_VERSION_NUM >= 90500
                           , Plan *outer_plan
#endif
                          )
{
	TdFdwRelationInfo *fpinfo = (TdFdwRelationInfo *) baserel->fdw_private;
	Index		scan_relid = baserel->relid;
	List	   *fdw_private;
	List	   *remote_conds = NIL;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *retrieved_attrs;
	TdFdwOption fdw_option;
	StringInfoData sql;
	ListCell   *lc;

	ExtractFdwOptions(fpinfo->table, &fdw_option);

	/*
	 * Separate the scan_clauses into those that can be executed remotely and
	 * those that can't.  baserestrictinfo clauses that were previously
	 * determined to be safe or unsafe by classifyConditions are shown in
	 * fpinfo->remote_conds and fpinfo->local_conds.  Anything else in the
	 * scan_clauses list will be a join clause, which we have to check for
	 * remote-safety.
	 *
	 * Note: the join clauses we see here should be the exact same ones
	 * previously examined by treasuredataGetForeignPaths.  Possibly it'd be worth
	 * passing forward the classification work done then, rather than
	 * repeating it here.
	 *
	 * This code must match "extract_actual_clauses(scan_clauses, false)"
	 * except for the additional decision about remote versus local execution.
	 * Note however that we don't strip the RestrictInfo nodes from the
	 * remote_conds list, since appendWhereClause expects a list of
	 * RestrictInfos.
	 */
	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		Assert(IsA(rinfo, RestrictInfo));

		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(fpinfo->remote_conds, rinfo))
		{
			remote_conds = lappend(remote_conds, rinfo);
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		}
		else if (list_member_ptr(fpinfo->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
		else if (is_foreign_expr(root, baserel, rinfo->clause))
		{
			remote_conds = lappend(remote_conds, rinfo);
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		}
		else
			local_exprs = lappend(local_exprs, rinfo->clause);
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	deparseSelectSql(&sql, root, baserel, fpinfo->attrs_used,
	                 &retrieved_attrs, fpinfo->query_engine_type);

	/* Use a SQL specified as 'query' option param instead if it's enabled */
	if (fdw_option.query != NULL)
	{
		initStringInfo(&sql);
		appendStringInfoString(&sql, "SELECT * FROM ( ");
		appendStringInfoString(&sql, fdw_option.query);
		appendStringInfoString(&sql, " ) result ");
	}

	if (remote_conds)
		appendWhereClause(&sql, root, baserel, remote_conds,
		                  true, &params_list, fpinfo->query_engine_type);

	/*
	 * Add FOR UPDATE/SHARE if appropriate.  We apply locking during the
	 * initial row fetch, rather than later on as is done for local tables.
	 * The extra roundtrips involved in trying to duplicate the local
	 * semantics exactly don't seem worthwhile (see also comments for
	 * RowMarkType).
	 *
	 * Note: because we actually run the query as a cursor, this assumes that
	 * DECLARE CURSOR ... FOR UPDATE is supported, which it isn't before 8.3.
	 */
	if (baserel->relid == root->parse->resultRelation &&
	        (root->parse->commandType == CMD_UPDATE ||
	         root->parse->commandType == CMD_DELETE))
	{
		elog(ERROR, "SELECT FOR UPDATE isn't supported");
	}
#if PG_VERSION_NUM >= 90500
	else
	{
		PlanRowMark *rc = get_plan_rowmark(root->rowMarks, baserel->relid);

		if (rc)
		{
			/*
			 * Relation is specified as a FOR UPDATE/SHARE target, so handle
			 * that.  (But we could also see LCS_NONE, meaning this isn't a
			 * target relation after all.)
			 *
			 * For now, just ignore any [NO] KEY specification, since (a) it's
			 * not clear what that means for a remote table that we don't have
			 * complete information about, and (b) it wouldn't work anyway on
			 * older remote servers.  Likewise, we don't worry about NOWAIT.
			 */
			switch (rc->strength)
			{
				case LCS_NONE:
					/* No locking needed */
					break;
				case LCS_FORKEYSHARE:
				case LCS_FORSHARE:
					elog(ERROR, "SELECT FOR SHARE isn't supported");
					break;
				case LCS_FORNOKEYUPDATE:
				case LCS_FORUPDATE:
					elog(ERROR, "SELECT FOR UPDATE isn't supported");
					break;
			}
		}
	}
#endif

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwScanPrivateIndex, above.
	 */
	fdw_private = list_make2(makeString(sql.data),
	                         retrieved_attrs);

	/*
	 * Create the ForeignScan node from target list, local filtering
	 * expressions, remote parameter expressions, and FDW private information.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist
	                        , local_exprs
	                        , scan_relid
	                        , params_list
	                        , fdw_private
#if PG_VERSION_NUM >= 90500
	                        , NIL	/* no custom tlist */
	                        , remote_exprs
	                        , outer_plan
#endif
	                       );
}

/*
 * treasuredataBeginForeignScan
 *		Initiate an executor scan of a foreign PostgreSQL table.
 */
static void
treasuredataBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	TdFdwScanState *fsstate;
	int			numParams;
	int			i;
	ListCell   *lc;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	fsstate = (TdFdwScanState *) palloc0(sizeof(TdFdwScanState));
	node->fdw_state = (void *) fsstate;

	/* Get info about foreign table. */
	fsstate->rel = node->ss.ss_currentRelation;

	fsstate->td_client = NULL;

	/* Get private info created by planner functions. */
	fsstate->query = strVal(list_nth(fsplan->fdw_private,
	                                 FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
	                           FdwScanPrivateRetrievedAttrs);

	/* Get info about foreign table. */
	fsstate->table = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));

	/* Create contexts for batches of tuples and per-tuple temp workspace. */
	fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
	                    "treasuredata_fdw temporary data",
	                    ALLOCSET_SMALL_MINSIZE,
	                    ALLOCSET_SMALL_INITSIZE,
	                    ALLOCSET_SMALL_MAXSIZE);

	/* Get info we'll need for input data conversion. */
	fsstate->attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(fsstate->rel));

	/* Prepare for output conversion of parameters used in remote query. */
	numParams = list_length(fsplan->fdw_exprs);
	fsstate->numParams = numParams;
	fsstate->param_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * numParams);

	i = 0;
	foreach(lc, fsplan->fdw_exprs)
	{
		Node	   *param_expr = (Node *) lfirst(lc);
		Oid			typefnoid;
		bool		isvarlena;

		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fsstate->param_flinfo[i]);
		i++;
	}

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require treasuredata_fdw to know more than is desirable
	 * about Param evaluation.)
	 */
	fsstate->param_exprs = (List *)
	                       ExecInitExpr((Expr *) fsplan->fdw_exprs,
	                                    (PlanState *) node);

	/*
	 * Allocate buffer for text form of query parameters, if any.
	 */
	if (numParams > 0)
		fsstate->param_values = (const char **) palloc0(numParams * sizeof(char *));
	else
		fsstate->param_values = NULL;
}

/*
 * treasuredataIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
static TupleTableSlot *
treasuredataIterateForeignScan(ForeignScanState *node)
{
	TdFdwScanState *fsstate = (TdFdwScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	HeapTuple	    tuple;;

	if (fsstate->td_client == NULL)
	{
		TdFdwOption fdw_option;
		ExtractFdwOptions(fsstate->table, &fdw_option);

		fsstate->fetch_all_column = fdw_option.query != NULL;

		fsstate->td_client = issueQuery(
		                         fdw_option.apikey,
		                         fdw_option.endpoint,
		                         fdw_option.query_engine,
		                         fdw_option.database,
		                         fsstate->query,
		                         fdw_option.query_download_dir);
	}

	tuple = make_tuple_from_result_row(
	            fsstate->td_client,
	            fsstate->fetch_all_column,
	            fsstate->rel,
	            fsstate->attinmeta,
	            fsstate->retrieved_attrs,
	            fsstate->temp_cxt);

	if (tuple == NULL)
	{
		return ExecClearTuple(slot);
	}

	/*
	 * Return the next tuple.
	 */
	ExecStoreTuple(tuple, slot, InvalidBuffer, false);

	return slot;
}

/*
 * treasuredataEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
treasuredataEndForeignScan(ForeignScanState *node)
{
	TdFdwScanState *fsstate = (TdFdwScanState *) node->fdw_state;

	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;

	releaseQueryResource(fsstate->td_client);

	fsstate->td_client = NULL;

	/* MemoryContexts will be deleted automatically. */
}

/*
 * treasuredataAddForeignUpdateTargets
 *		Add resjunk column(s) needed for update/delete on a foreign table
 */
static void
treasuredataAddForeignUpdateTargets(Query *parsetree,
                                    RangeTblEntry *target_rte,
                                    Relation target_relation)
{
	Var		   *var;
	const char *attrname;
	TargetEntry *tle;

	/*
	 * In treasuredata_fdw, what we need is the ctid, same as for a regular table.
	 */

	/* Make a Var representing the desired value */
	var = makeVar(parsetree->resultRelation,
	              SelfItemPointerAttributeNumber,
	              TIDOID,
	              -1,
	              InvalidOid,
	              0);

	/* Wrap it in a resjunk TLE with the right name ... */
	attrname = "ctid";

	tle = makeTargetEntry((Expr *) var,
	                      list_length(parsetree->targetList) + 1,
	                      pstrdup(attrname),
	                      true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
}

/*
 * treasuredataPlanForeignModify
 *		Plan an insert/update/delete operation on a foreign table
 *
 * Note: currently, the plan tree generated for UPDATE/DELETE will always
 * include a ForeignScan that retrieves ctids (using SELECT FOR UPDATE)
 * and then the ModifyTable node will have to execute individual remote
 * UPDATE/DELETE commands.  If there are no local conditions or joins
 * needed, it'd be better to let the scan node do UPDATE/DELETE RETURNING
 * and then do nothing at ModifyTable.  Room for future optimization ...
 */
static List *
treasuredataPlanForeignModify(PlannerInfo *root,
                              ModifyTable *plan,
                              Index resultRelation,
                              int subplan_index)
{
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	StringInfoData sql;
	List	   *targetAttrs = NIL;
	List	   *returningList = NIL;
	List	   *retrieved_attrs = NIL;
	bool		doNothing = false;

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, we transmit only columns that were explicitly
	 * targets of the UPDATE, so as to avoid unnecessary data transmission.
	 * (We can't do that for INSERT since we would miss sending default values
	 * for columns not listed in the source statement.)
	 */
	if (operation == CMD_INSERT)
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else
	{
		elog(ERROR, "This FDW doesn't support UPDATE/DELETE");
	}

	if (plan->returningLists)
		elog(ERROR, "This FDW doesn't support RETURNING");

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
		case CMD_INSERT:
			deparseInsertSql(&sql, root, resultRelation, rel,
			                 targetAttrs, doNothing, returningList,
			                 &retrieved_attrs);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	heap_close(rel, NoLock);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */
	return list_make4(makeString(sql.data),
	                  targetAttrs,
	                  makeInteger((retrieved_attrs != NIL)),
	                  retrieved_attrs);
}

/*
 * treasuredataBeginForeignModify
 *		Begin an insert/update/delete operation on a foreign table
 */
static void
treasuredataBeginForeignModify(ModifyTableState *mtstate,
                               ResultRelInfo *resultRelInfo,
                               List *fdw_private,
                               int subplan_index,
                               int eflags)
{
	TdFdwModifyState *fmstate;
	EState	   *estate = mtstate->ps.state;
	CmdType		operation = mtstate->operation;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	AttrNumber	n_params;
	Oid			typefnoid;
	bool		isvarlena;
	ListCell   *lc;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Begin constructing TdFdwModifyState. */
	fmstate = (TdFdwModifyState *) palloc0(sizeof(TdFdwModifyState));
	fmstate->rel = rel;

	/* Deconstruct fdw_private data. */
	fmstate->query = strVal(list_nth(fdw_private,
	                                 FdwModifyPrivateUpdateSql));
	fmstate->target_attrs = (List *) list_nth(fdw_private,
	                        FdwModifyPrivateTargetAttnums);

	/* Create context for per-tuple temp workspace. */
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
	                    "treasuredata_fdw temporary data",
	                    ALLOCSET_SMALL_MINSIZE,
	                    ALLOCSET_SMALL_INITSIZE,
	                    ALLOCSET_SMALL_MAXSIZE);

	/* Prepare for output conversion of parameters used in prepared stmt. */
	n_params = list_length(fmstate->target_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;

	// if (operation == CMD_INSERT || operation == CMD_UPDATE)
	if (operation == CMD_INSERT)
	{
		char **column_types = palloc0(sizeof(char *) * list_length(fmstate->target_attrs));
		char **column_names = palloc0(sizeof(char *) * list_length(fmstate->target_attrs));

		fmstate->table = GetForeignTable(RelationGetRelid(rel));
		ExtractFdwOptions(fmstate->table, &fmstate->fdw_option);

		if (fmstate->fdw_option.query != NULL)
		{
			elog(ERROR, "This FDW with 'query' option parameter doesn't support INSERT");
		}

		/* Set up for remaining transmittable parameters */
		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);

			Assert(!attr->attisdropped);

			getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
			fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);

			elog(DEBUG1, "attnum=%d, typefnoid=%d, isvarlena=%d, attname=%s, atttypid=%d, p_flinfo.fn_oid=%d",
			     attnum, typefnoid, isvarlena, NameStr(attr->attname), attr->atttypid, fmstate->p_flinfo[fmstate->p_nums].fn_oid);

			if (
			    typefnoid == 39 ||      /* small(int|serial) */
			    typefnoid == 43 ||      /* (integer|serial) */
			    typefnoid == 461        /* big(int|serial) */
			)
			{
				/* Integer */
				column_types[fmstate->p_nums] = "int";
			}
			else if (
			    typefnoid == 201 ||     /* (float4|real) */
			    typefnoid == 215 ||     /* (float8|double precision) */
			    typefnoid == 1702       /* (decimal|numeric) */
			)
			{
				/* Float */
				column_types[fmstate->p_nums] = "float";
			}
			else if (
			    typefnoid == 47 ||     /* text */
			    typefnoid == 1045 ||   /* varchar */
			    typefnoid == 1047      /* char */
			)
			{
				/* String */
				column_types[fmstate->p_nums] = "string";
			}
			// TODO: Take care of map type
			else
			{
				elog(ERROR, "Unsupported typefnoid: %d", typefnoid);
			}

			{
				char *attname = NameStr(attr->attname);
				int len_with_null_char = strlen(attname) + 1;
				column_names[fmstate->p_nums] = palloc0(len_with_null_char);
				strncpy(column_names[fmstate->p_nums], attname, len_with_null_char);
			}

			fmstate->p_nums++;
		}

		fmstate->column_types = (const char **) column_types;
		fmstate->column_names = (const char **) column_names;

		if (fmstate->fdw_option.atomic_import)
		{
			size_t len = strlen(fmstate->fdw_option.table) + 1 + 10 + 1 + 10 + 1;
			fmstate->tmp_table_name = palloc(len);
			snprintf(fmstate->tmp_table_name, len,
			         "%s_%d_%ld", fmstate->fdw_option.table,
			         getpid(), time(NULL));

			createTable(
			    fmstate->fdw_option.apikey,
			    fmstate->fdw_option.endpoint,
			    fmstate->fdw_option.database,
			    fmstate->tmp_table_name);
		}
		else
		{
			fmstate->tmp_table_name = NULL;
		}

		/* Setup td-client */
		fmstate->td_client = importBegin(
		                         fmstate->fdw_option.apikey,
		                         fmstate->fdw_option.endpoint,
		                         fmstate->fdw_option.database,
		                         fmstate->fdw_option.atomic_import ?
		                         fmstate->tmp_table_name : fmstate->fdw_option.table,
		                         fmstate->p_nums,
		                         fmstate->column_types,
		                         fmstate->column_names);
	}
	else
	{
		elog(ERROR, "This FDW doesn't support RETURNING");
	}

	Assert(fmstate->p_nums <= n_params);

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * treasuredataExecForeignInsert
 *		Insert one row into a foreign table
 */
static TupleTableSlot *
treasuredataExecForeignInsert(EState *estate,
                              ResultRelInfo *resultRelInfo,
                              TupleTableSlot *slot,
                              TupleTableSlot *planSlot)
{
	TdFdwModifyState *fmstate = (TdFdwModifyState *) resultRelInfo->ri_FdwState;
	size_t written_len;
	const char **p_values;

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate, NULL, slot);
	written_len = importAppend(fmstate->td_client, p_values);

	MemoryContextReset(fmstate->temp_cxt);

	if (written_len > fmstate->fdw_option.import_file_size)
	{
		/* If the written data size gets too large, upload the file and setup td-client agein */
		importCommit(fmstate->td_client);
		fmstate->td_client = NULL;

		fmstate->td_client = importBegin(
		                         fmstate->fdw_option.apikey,
		                         fmstate->fdw_option.endpoint,
		                         fmstate->fdw_option.database,
		                         fmstate->fdw_option.atomic_import ?
		                         fmstate->tmp_table_name : fmstate->fdw_option.table,
		                         fmstate->p_nums,
		                         fmstate->column_types,
		                         fmstate->column_names);
	}

	return slot;
}

/*
 * treasuredataEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
treasuredataEndForeignModify(EState *estate,
                             ResultRelInfo *resultRelInfo)
{
	TdFdwModifyState *fmstate = (TdFdwModifyState *) resultRelInfo->ri_FdwState;

	/* If fmstate is NULL, we are in EXPLAIN; nothing to do */
	if (fmstate == NULL)
		return;

	// Upload the chunk file to Treasure Data
	importCommit(fmstate->td_client);
	fmstate->td_client = NULL;

	if (fmstate->fdw_option.atomic_import)
	{
		StringInfoData sql;

		/* Append schema to the temp table to make the following INSERT INTO be successful */
		appendTableSchema(
		    fmstate->fdw_option.apikey,
		    fmstate->fdw_option.endpoint,
		    fmstate->fdw_option.database,
		    fmstate->tmp_table_name,
		    fmstate->p_nums,
		    fmstate->column_types,
		    fmstate->column_names);

		/* Move imported data from the temp table to the target table with INSERT INTO */
		initStringInfo(&sql);
		if (strcmp(fmstate->fdw_option.query_engine, "hive") == 0)
		{
			int i;
			appendStringInfoString(&sql, "INSERT INTO ");
			appendStringInfoString(&sql, "TABLE ");
			appendStringInfoString(&sql, fmstate->fdw_option.table);
			appendStringInfoString(&sql, " SELECT ");
			for (i = 0; i < fmstate->p_nums; i++)
			{
				if (i != 0)
				{
					appendStringInfoString(&sql, ", ");
				}
				appendStringInfoString(&sql, "`");
				appendStringInfoString(&sql, fmstate->column_names[i]);
				appendStringInfoString(&sql, "`");
			}
			appendStringInfoString(&sql, " FROM ");
			appendStringInfoString(&sql, fmstate->tmp_table_name);
		}
		else if (strcmp(fmstate->fdw_option.query_engine, "presto") == 0)
		{
			appendStringInfoString(&sql, "INSERT INTO ");
			appendStringInfoString(&sql, fmstate->fdw_option.table);
			appendStringInfoString(&sql, " SELECT * FROM ");
			appendStringInfoString(&sql, fmstate->tmp_table_name);
		}

		/* We'd better create `issueUpdate` that doesn't return results and use here */
		issueQuery(
		    fmstate->fdw_option.apikey,
		    fmstate->fdw_option.endpoint,
		    fmstate->fdw_option.query_engine,
		    fmstate->fdw_option.database,
		    sql.data,
		    fmstate->fdw_option.query_download_dir);

		deleteTable(
		    fmstate->fdw_option.apikey,
		    fmstate->fdw_option.endpoint,
		    fmstate->fdw_option.database,
		    fmstate->tmp_table_name);
	}
}

/*
 * estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan
 *
 * We assume that all the baserestrictinfo clauses will be applied, plus
 * any join clauses listed in join_conds.
 */
static void
estimate_path_cost_size(PlannerInfo *root,
                        RelOptInfo *baserel,
                        List *join_conds,
                        double *p_rows, int *p_width,
                        Cost *p_startup_cost, Cost *p_total_cost)
{
	TdFdwRelationInfo *fpinfo = (TdFdwRelationInfo *) baserel->fdw_private;
	double		rows;
	double		retrieved_rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	Cost		run_cost;
	Cost		cpu_per_tuple;

	/*
	 * We don't support join conditions in this mode (hence, no
	 * parameterized paths can be made).
	 */
	Assert(join_conds == NIL);

	/* Use rows/width estimates made by set_baserel_size_estimates. */
	rows = baserel->rows;
#if PG_VERSION_NUM >= 90600
	width = baserel->reltarget->width;
#else
	width = baserel->width;
#endif

	/*
	 * Back into an estimate of the number of retrieved rows.  Just in
	 * case this is nuts, clamp to at most baserel->tuples.
	 */
	retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
	retrieved_rows = Min(retrieved_rows, baserel->tuples);

	/*
	 * Cost as though this were a seqscan, which is pessimistic.  We
	 * effectively imagine the local_conds are being evaluated remotely,
	 * too.
	 */
	startup_cost = 0;
	run_cost = 0;
	run_cost += seq_page_cost * baserel->pages;

	startup_cost += baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * baserel->tuples;

	total_cost = startup_cost + run_cost;

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}

/*
 * convert_prep_stmt_params
 *		Create array of text strings representing parameter values
 *
 * tupleid is ctid to send, or NULL if none
 * slot is slot to get remaining parameters from, or NULL if none
 *
 * Data is constructed in temp_cxt; caller should reset that after use.
 */
static const char **
convert_prep_stmt_params(TdFdwModifyState *fmstate,
                         ItemPointer tupleid,
                         TupleTableSlot *slot)
{
	const char **p_values;
	int			pindex = 0;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	p_values = (const char **) palloc(sizeof(char *) * fmstate->p_nums);

	/* 1st parameter should be ctid, if it's in use */
	if (tupleid != NULL)
	{
		/* don't need set_transmission_modes for TID output */
		p_values[pindex] = OutputFunctionCall(&fmstate->p_flinfo[pindex],
		                                      PointerGetDatum(tupleid));
		pindex++;
	}

	/* get following parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		/*
		int			nestlevel;
		*/
		ListCell   *lc;

		/*
		nestlevel = set_transmission_modes();
		*/

		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Datum		value;
			bool		isnull;

			value = slot_getattr(slot, attnum, &isnull);
			if (isnull)
				p_values[pindex] = NULL;
			else
				p_values[pindex] = OutputFunctionCall(&fmstate->p_flinfo[pindex],
				                                      value);
			pindex++;
		}

		/*
		reset_transmission_modes(nestlevel);
		*/
	}

	Assert(pindex == fmstate->p_nums);

	MemoryContextSwitchTo(oldcontext);

	return p_values;
}

#if PG_VERSION_NUM >= 90500
/*
 * Import a foreign schema
 */
static List *
treasuredataImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	List		*commands = NIL;
	char		*endpoint = NULL;
	char		*query_engine = NULL;
	char		*apikey = NULL;
	StringInfoData buf;
	ListCell	*lc = NULL;
	table_schemas_t *tables = NULL;
	int			i, j;

	/* Parse statement options */
	foreach(lc, stmt->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "apikey") == 0)
			apikey = defGetString(def);
		else if (strcmp(def->defname, "endpoint") == 0)
			endpoint = defGetString(def);
		else if (strcmp(def->defname, "query_engine") == 0)
			query_engine = defGetString(def);
		else
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			         errmsg("invalid option \"%s\"", def->defname)));
	}

	/* Validate options */
	if (apikey == NULL)
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
		         errmsg("apikey is required for treasuredata_fdw foreign tables")));
	if (query_engine == NULL)
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
		         errmsg("query_engine is required for treasuredata_fdw foreign tables")));

	ereport(DEBUG1,
	        (errmsg("apikey.len: %zu, endpoint: %s, query_engine: %s",
	                strlen(apikey), endpoint, query_engine)));
	ereport(DEBUG1,
	        (errmsg("remote_database: %s, server: %s",
	                stmt->remote_schema, stmt->server_name)));

	/* Create workspace for strings */
	initStringInfo(&buf);

	/* Note that imported tables will NOT include time column
	   since TD API does not return time column.*/
	tables = getTableSchemas(apikey, endpoint, stmt->remote_schema);

	for (i = 0; i < tables->numtables; i++)
	{
		table_schema_t *table = tables->tables[i];
		bool		first_item = true;

		resetStringInfo(&buf);
		appendStringInfo(&buf, "CREATE FOREIGN TABLE %s (\n",
		                 quote_identifier(table->name));

		for (j = 0; j < table->numcols; j++)
		{
			char	   *colname = table->colnames[j];
			char	   *coltype = table->coltypes[j];

			if (first_item)
				first_item = false;
			else
				appendStringInfoString(&buf, ",\n");

			/* Print column name and type */
			appendStringInfo(&buf, "  %s %s",
			                 quote_identifier(colname),
			                 coltype);
		}

		/*
		 * Add server name and table-level options.
		 */
		appendStringInfo(&buf, "\n) SERVER %s\nOPTIONS (",
		                 quote_identifier(stmt->server_name));

		appendStringInfoString(&buf, "apikey ");
		deparseStringLiteral(&buf, apikey);
		if (endpoint != NULL)
		{
			appendStringInfoString(&buf, ", endpoint ");
			deparseStringLiteral(&buf, endpoint);
		}
		appendStringInfoString(&buf, ", database ");
		deparseStringLiteral(&buf, stmt->remote_schema);
		appendStringInfoString(&buf, ", query_engine ");
		deparseStringLiteral(&buf, query_engine);
		appendStringInfoString(&buf, ", table ");
		deparseStringLiteral(&buf, table->name);

		appendStringInfoString(&buf, ");");

		commands = lappend(commands, pstrdup(buf.data));
	}

	ereport(DEBUG1,
	        (errmsg("number of import commands: %d",
	                list_length(commands))));

	return commands;
}
#endif


#if PG_VERSION_NUM >= 90500
/*
 * treasuredataGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 */
static void
treasuredataGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
							 RelOptInfo *input_rel, RelOptInfo *output_rel,
							 void *extra)
{
	TdFdwRelationInfo *fpinfo;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((TdFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if ((stage != UPPERREL_GROUP_AGG &&
		 stage != UPPERREL_ORDERED &&
		 stage != UPPERREL_FINAL) ||
		output_rel->fdw_private)
		return;

	fpinfo = (TdFdwRelationInfo *) palloc0(sizeof(TdFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	output_rel->fdw_private = fpinfo;

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
			add_foreign_grouping_paths(root, input_rel, output_rel,
									   (GroupPathExtraData *) extra);
			break;
		case UPPERREL_ORDERED:
            /*
			add_foreign_ordered_paths(root, input_rel, output_rel);
			break;
            */
			elog(DEBUG1, "UPPERREL_ORDERED isn't supported");
            return;
		case UPPERREL_FINAL:
            /*
			add_foreign_final_paths(root, input_rel, output_rel,
									(FinalPathExtraData *) extra);
			break;
            */
			elog(DEBUG1, "UPPERREL_FINAL isn't supported");
            return;
		default:
			elog(ERROR, "unexpected upper relation: %d", (int) stage);
			break;
	}
}

/*
 * add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
						   RelOptInfo *grouped_rel,
						   GroupPathExtraData *extra)
{
	Query	   *parse = root->parse;
	TdFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	TdFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

	Assert(extra->patype == PARTITIONWISE_AGGREGATE_NONE ||
		   extra->patype == PARTITIONWISE_AGGREGATE_FULL);

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
//	fpinfo->user = ifpinfo->user;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * Assess if it is safe to push down aggregation and grouping.
	 *
	 * Use HAVING qual from extra. In case of child partition, it will have
	 * translated Vars.
	 */
	if (!foreign_grouping_ok(root, grouped_rel, extra->havingQual))
		return;

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  (Currently we create just a single
	 * path here, but in future it would be possible that we build more paths
	 * such as pre-sorted paths as in postgresGetForeignPaths and
	 * postgresGetForeignJoinPaths.)  The best we can do for these conditions
	 * is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 0,
													 JOIN_INNER,
													 NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/* Estimate the cost of push down */
	estimate_path_cost_size(root, grouped_rel, NIL, /* TODO: NIL, NULL, */
							&rows, &width, &startup_cost, &total_cost);

	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/* Create and add foreign path to the grouping relation. */
	grouppath = create_foreignscan_path(root,
                                   grouped_rel,
								   grouped_rel->reltarget,
                                   rows,
                                   startup_cost,
								   total_cost,
	                               NIL,     /* no pathkeys */
                                   NULL,    /* no required_outer */
	                               NULL,
	                               NIL);    /* no fdw_private list */

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}

/*
 * Merge FDW options from input relations into a new set of options for a join
 * or an upper rel.
 *
 * For a join relation, FDW-specific information about the inner and outer
 * relations is provided using fpinfo_i and fpinfo_o. For an upper relation,
 * fpinfo_o provides the information for the input relation; fpinfo_i is
 * expected to NULL.
 */
static void
merge_fdw_options(TdFdwRelationInfo *fpinfo,
				  const TdFdwRelationInfo *fpinfo_o,
				  const TdFdwRelationInfo *fpinfo_i)
{
	/* We must always have fpinfo_o. */
	Assert(fpinfo_o);

	/* fpinfo_i may be NULL, but if present the servers must both match. */
	Assert(!fpinfo_i ||
		   fpinfo_i->server->serverid == fpinfo_o->server->serverid);

	/*
	 * Copy the server specific FDW options.  (For a join, both relations come
	 * from the same server, so the server options should have the same value
	 * for both relations.)
	 */
	fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;
    /*
	fpinfo->shippable_extensions = fpinfo_o->shippable_extensions;
	fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate;
	fpinfo->fetch_size = fpinfo_o->fetch_size;
    */

#if 0
	/* Merge the table level options from either side of the join. */
	if (fpinfo_i)
	{
		/*
		 * We'll prefer to use remote estimates for this join if any table
		 * from either side of the join is using remote estimates.  This is
		 * most likely going to be preferred since they're already willing to
		 * pay the price of a round trip to get the remote EXPLAIN.  In any
		 * case it's not entirely clear how we might otherwise handle this
		 * best.
		 */
		fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate ||
			fpinfo_i->use_remote_estimate;

		/*
		 * Set fetch size to maximum of the joining sides, since we are
		 * expecting the rows returned by the join to be proportional to the
		 * relation sizes.
		 */
		fpinfo->fetch_size = Max(fpinfo_o->fetch_size, fpinfo_i->fetch_size);
	}
#endif
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to TdFdwRelationInfo of the input relation.
 */
static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
					Node *havingQual)
{
	Query	   *query = root->parse;
	TdFdwRelationInfo *fpinfo = (TdFdwRelationInfo *) grouped_rel->fdw_private;
	PathTarget *grouping_target = grouped_rel->reltarget;
	TdFdwRelationInfo *ofpinfo;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;

	/* We currently don't support pushing Grouping Sets. */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (TdFdwRelationInfo *) fpinfo->outerrel->fdw_private;

	/*
	 * If underlying scan relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Examine grouping expressions, as well as other expressions we'd need to
	 * compute, and check whether they are safe to push down to the foreign
	 * server.  All GROUP BY expressions will be part of the grouping target
	 * and thus there is no need to search for them separately.  Add grouping
	 * expressions into target list which will be passed to foreign server.
	 *
	 * A tricky fine point is that we must not put any expression into the
	 * target list that is just a foreign param (that is, something that
	 * deparse.c would conclude has to be sent to the foreign server).  If we
	 * do, the expression will also appear in the fdw_exprs list of the plan
	 * node, and setrefs.c will get confused and decide that the fdw_exprs
	 * entry is actually a reference to the fdw_scan_tlist entry, resulting in
	 * a broken plan.  Somewhat oddly, it's OK if the expression contains such
	 * a node, as long as it's not at top level; then no match is possible.
	 */
	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell   *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any GROUP BY expression is not shippable, then we cannot
			 * push down aggregation to the foreign server.
			 */
			if (!is_foreign_expr(root, grouped_rel, expr))
				return false;

/* TODO */
#if 0
			/*
			 * If it would be a foreign param, we can't put it into the tlist,
			 * so we have to fail.
			 */
			if (is_foreign_param(root, grouped_rel, expr))
				return false;
#endif

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/*
			 * Non-grouping expression we need to compute.  Can we ship it
			 * as-is to the foreign server?
			 */
			if (is_foreign_expr(root, grouped_rel, expr)
                    /* TODO: 
                    && !is_foreign_param(root, grouped_rel, expr)
                    */
                )
			{
				/* Yes, so add to tlist as-is; OK to suppress duplicates */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				/* Not pushable as a whole; extract its Vars and aggregates */
				List	   *aggvars;

				aggvars = pull_var_clause((Node *) expr,
										  PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the foreign server.  (We
				 * don't have to check is_foreign_param, since that certainly
				 * won't return true for any such expression.)
				 */
				if (!is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain Vars
				 * outside an aggregate can be ignored, because they should be
				 * either same as some GROUP BY column or part of some GROUP
				 * BY expression.  In either case, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * including plain Vars in the tlist when they do not match a
				 * GROUP BY column would cause the foreign server to complain
				 * that the shipped query is invalid.
				 */
				foreach(l, aggvars)
				{
					Expr	   *expr = (Expr *) lfirst(l);

					if (IsA(expr, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(expr));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable HAVING clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (havingQual)
	{
		ListCell   *lc;

		foreach(lc, (List *) havingQual)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
			rinfo = make_restrictinfo(expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
			if (is_foreign_expr(root, grouped_rel, expr))
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List	   *aggvars = NIL;
		ListCell   *lc;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.  Again, we need not check
			 * is_foreign_param for a foreign aggregate.
			 */
			if (IsA(expr, Aggref))
			{
				if (!is_foreign_expr(root, grouped_rel, expr))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

#if 0
	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.  Note that the decoration we add
	 * to the base relation name mustn't include any digits, or it'll confuse
	 * postgresExplainForeignScan.
	 */
	fpinfo->relation_name = psprintf("Aggregate on (%s)",
									 ofpinfo->relation_name);
#endif

	return true;
}
#endif

/*
 * Create a tuple from the specified row of the PGresult.
 *
 * rel is the local representation of the foreign table, attinmeta is
 * conversion data for the rel's tupdesc, and retrieved_attrs is an
 * integer list of the table column numbers present in the PGresult.
 * temp_context is a working context that can be reset after each tuple.
 */
static HeapTuple
make_tuple_from_result_row(void *td_client,
                           bool fetch_all_column,
                           Relation rel,
                           AttInMetadata *attinmeta,
                           List *retrieved_attrs,
                           MemoryContext temp_context)
{
	HeapTuple	tuple;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Datum	   *values;
	char      **result_values;
	bool	   *nulls;
	ItemPointer ctid = NULL;
	ConversionLocation errpos;
	ErrorContextCallback errcallback;
	MemoryContext oldcontext;
	ListCell   *lc;
	int			j;

	// Assert(row < PQntuples(res));

	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	oldcontext = MemoryContextSwitchTo(temp_context);

	values = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
	result_values = (char **) palloc0(tupdesc->natts * sizeof(char *));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	/*
	 * Set up and install callback to report where conversion error occurs.
	 */
	errpos.rel = rel;
	errpos.cur_attno = 0;
	errcallback.callback = conversion_error_callback;
	errcallback.arg = (void *) &errpos;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	if (fetchResultRow(td_client, tupdesc->natts, result_values) != 0)
	{
		error_context_stack = errcallback.previous;
		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(temp_context);
		return NULL;
	}

	/*
	 * i indexes columns in the relation, j indexes columns in the result from Treasure Data.
	 */
	j = 0;
	foreach(lc, retrieved_attrs)
	{
		int			i = lfirst_int(lc);
		int        result_index;
		char	   *valstr;

		if (fetch_all_column)
		{
			if (i <= 0)
			{
				elog(ERROR, "This FDW with 'query' option param doesn't support special attributes");
			}
			Assert(i <= tupdesc->natts);
			result_index = i - 1;
		}
		else
		{
			result_index = j;
		}

		valstr = result_values[result_index];

		/* convert value to internal representation */
		if (i > 0)
		{
			/* ordinary column */
			Assert(i <= tupdesc->natts);
			nulls[i - 1] = (valstr == NULL);
			/* Apply the input function even to nulls, to support domains */
			errpos.cur_attno = i;
			values[i - 1] = InputFunctionCall(&attinmeta->attinfuncs[i - 1],
			                                  valstr,
			                                  attinmeta->attioparams[i - 1],
			                                  attinmeta->atttypmods[i - 1]);
			errpos.cur_attno = 0;
		}
		else if (i == SelfItemPointerAttributeNumber)
		{
			/* ctid --- note we ignore any other system column in result */
			if (valstr != NULL)
			{
				Datum		datum;

				datum = DirectFunctionCall1(tidin, CStringGetDatum(valstr));
				ctid = (ItemPointer) DatumGetPointer(datum);
			}
		}

		j++;
	}

	/* Uninstall error context callback. */
	error_context_stack = errcallback.previous;

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/*
	 * If we have a CTID to return, install it in both t_self and t_ctid.
	 * t_self is the normal place, but if the tuple is converted to a
	 * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
	 * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
	 */
	if (ctid)
		tuple->t_self = tuple->t_data->t_ctid = *ctid;

	/* Clean up */
	MemoryContextReset(temp_context);

	return tuple;
}

/*
 * Callback function which is called when error occurs during column value
 * conversion.  Print names of column and relation.
 */
static void
conversion_error_callback(void *arg)
{
	ConversionLocation *errpos = (ConversionLocation *) arg;
	TupleDesc	tupdesc = RelationGetDescr(errpos->rel);

	if (errpos->cur_attno > 0 && errpos->cur_attno <= tupdesc->natts)
		errcontext("column \"%s\" of foreign table \"%s\"",
		           NameStr(TupleDescAttr(tupdesc, errpos->cur_attno - 1)->attname),
		           RelationGetRelationName(errpos->rel));
}

