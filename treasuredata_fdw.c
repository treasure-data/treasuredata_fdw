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
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"

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

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;
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
	FdwScanPrivateRetrievedAttrs,
	FdwScanPrivateForeignTable
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

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	int			fetch_ct_2;		/* Min(# of fetches done, 2) */
	bool		eof_reached;	/* true if last fetch reached EOF */

	/* working memory contexts */
	MemoryContext batch_cxt;	/* context holding current batch of tuples */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */

	/* Cached catalog information. */
	ForeignTable *table;
} TdFdwScanState;

#if 0
/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct TdFdwModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* for remote query execution */
	PGconn	   *conn;			/* connection for the scan */
	char	   *p_name;			/* name of prepared statement, if created */

	/* extracted fdw_private data */
	char	   *query;			/* text of INSERT/UPDATE/DELETE command */
	List	   *target_attrs;	/* list of target attribute numbers */
	bool		has_returning;	/* is there a RETURNING clause? */
	List	   *retrieved_attrs;	/* attr numbers retrieved by RETURNING */

	/* info about parameters for prepared statement */
	AttrNumber	ctidAttno;		/* attnum of input resjunk ctid column */
	int			p_nums;			/* number of parameters to transmit */
	FmgrInfo   *p_flinfo;		/* output conversion functions for them */

	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} TdFdwModifyState;

/*
 * Workspace for analyzing a foreign table.
 */
typedef struct TdFdwAnalyzeState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */
	List	   *retrieved_attrs;	/* attr numbers retrieved by query */

	/* collected sample rows */
	HeapTuple  *rows;			/* array of size targrows */
	int			targrows;		/* target # of sample rows */
	int			numrows;		/* # of sample rows collected */

	/* for random sampling */
	double		samplerows;		/* # of rows fetched */
	double		rowstoskip;		/* # of rows to skip before next sample */
	ReservoirStateData rstate;	/* state for reservoir sampling */

	/* working memory contexts */
	MemoryContext anl_cxt;		/* context for per-analyze lifespan data */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} TdFdwAnalyzeState;
#endif
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
        List *scan_clauses,
        Plan *outer_plan);
static void treasuredataBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *treasuredataIterateForeignScan(ForeignScanState *node);
// static void treasuredataReScanForeignScan(ForeignScanState *node);
static void treasuredataEndForeignScan(ForeignScanState *node);
#if 0
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
static TupleTableSlot *treasuredataExecForeignUpdate(EState *estate,
        ResultRelInfo *resultRelInfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);
static TupleTableSlot *treasuredataExecForeignDelete(EState *estate,
        ResultRelInfo *resultRelInfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);
static void treasuredataEndForeignModify(EState *estate,
        ResultRelInfo *resultRelInfo);
static int	treasuredataIsForeignRelUpdatable(Relation rel);
static void treasuredataExplainForeignScan(ForeignScanState *node,
        ExplainState *es);
static void treasuredataExplainForeignModify(ModifyTableState *mtstate,
        ResultRelInfo *rinfo,
        List *fdw_private,
        int subplan_index,
        ExplainState *es);
static bool treasuredataAnalyzeForeignTable(Relation relation,
        AcquireSampleRowsFunc *func,
        BlockNumber *totalpages);
static List *treasuredataImportForeignSchema(ImportForeignSchemaStmt *stmt,
        Oid serverOid);
#endif
/*
 * Helper functions
 */
static void estimate_path_cost_size(PlannerInfo *root,
                                    RelOptInfo *baserel,
                                    List *join_conds,
                                    double *p_rows, int *p_width,
                                    Cost *p_startup_cost, Cost *p_total_cost);
#if 0
static bool ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
                                      EquivalenceClass *ec, EquivalenceMember *em,
                                      void *arg);
static void fetch_more_data(ForeignScanState *node);
static void close_cursor(PGconn *conn, unsigned int cursor_number);
static void prepare_foreign_modify(TdFdwModifyState *fmstate);
static const char **convert_prep_stmt_params(TdFdwModifyState *fmstate,
        ItemPointer tupleid,
        TupleTableSlot *slot);
static void store_returning_result(TdFdwModifyState *fmstate,
                                   TupleTableSlot *slot, PGresult *res);
static int treasuredataAcquireSampleRowsFunc(Relation relation, int elevel,
        HeapTuple *rows, int targrows,
        double *totalrows,
        double *totaldeadrows);
static void analyze_row_processor(PGresult *res, int row,
                                  TdFdwAnalyzeState *astate);
#endif
static HeapTuple make_tuple_from_result_row(void *td_client,
        Relation rel,
        AttInMetadata *attinmeta,
        List *retrieved_attrs,
        MemoryContext temp_context);
static void conversion_error_callback(void *arg);


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
//	routine->ReScanForeignScan = treasuredataReScanForeignScan;
	routine->EndForeignScan = treasuredataEndForeignScan;
#if 0
	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = treasuredataAddForeignUpdateTargets;
	routine->PlanForeignModify = treasuredataPlanForeignModify;
	routine->BeginForeignModify = treasuredataBeginForeignModify;
	routine->ExecForeignInsert = treasuredataExecForeignInsert;
	routine->ExecForeignUpdate = treasuredataExecForeignUpdate;
	routine->ExecForeignDelete = treasuredataExecForeignDelete;
	routine->EndForeignModify = treasuredataEndForeignModify;
	routine->IsForeignRelUpdatable = treasuredataIsForeignRelUpdatable;

	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = treasuredataExplainForeignScan;
	routine->ExplainForeignModify = treasuredataExplainForeignModify;

	/* Support functions for ANALYZE */
	routine->AnalyzeForeignTable = treasuredataAnalyzeForeignTable;

	/* Support functions for IMPORT FOREIGN SCHEMA */
	routine->ImportForeignSchema = treasuredataImportForeignSchema;
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
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid,
	               &fpinfo->attrs_used);
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
		    (10 * BLCKSZ) / (baserel->width + MAXALIGN(SizeofHeapTupleHeader));
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
	                               fpinfo->rows,
	                               fpinfo->startup_cost,
	                               fpinfo->total_cost,
	                               NIL, /* no pathkeys */
	                               NULL,		/* no outer rel either */
	                               NULL,		/* no extra plan */
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
                           List *scan_clauses,
                           Plan *outer_plan)
{
	TdFdwRelationInfo *fpinfo = (TdFdwRelationInfo *) baserel->fdw_private;
	Index		scan_relid = baserel->relid;
	List	   *fdw_private;
	List	   *remote_conds = NIL;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *retrieved_attrs;
	StringInfoData sql;
	ListCell   *lc;

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
	                 &retrieved_attrs);
	if (remote_conds)
		appendWhereClause(&sql, root, baserel, remote_conds,
		                  true, &params_list);

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

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwScanPrivateIndex, above.
	 */
	fdw_private = list_make3(makeString(sql.data),
	                         retrieved_attrs,
	                         fpinfo->table
	                        );

	/*
	 * Create the ForeignScan node from target list, local filtering
	 * expressions, remote parameter expressions, and FDW private information.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
	                        local_exprs,
	                        scan_relid,
	                        params_list,
	                        fdw_private,
	                        NIL,	/* no custom tlist */
	                        remote_exprs,
	                        outer_plan);
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
	RangeTblEntry *rte;
	Oid			userid;
	ForeignTable *table;
	ForeignServer *server;
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

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
	rte = rt_fetch(fsplan->scan.scanrelid, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	fsstate->rel = node->ss.ss_currentRelation;
	table = GetForeignTable(RelationGetRelid(fsstate->rel));
	server = GetForeignServer(table->serverid);

	/*
	 * TODO: Support multiple requests
	 */
	fsstate->td_client = NULL;

	/* Get private info created by planner functions. */
	fsstate->query = strVal(list_nth(fsplan->fdw_private,
	                                 FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
	                           FdwScanPrivateRetrievedAttrs);
	fsstate->table = (ForeignTable *) list_nth(fsplan->fdw_private,
	                 FdwScanPrivateForeignTable);


	/* Create contexts for batches of tuples and per-tuple temp workspace. */
	fsstate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
	                     "treasuredata_fdw tuple data",
	                     ALLOCSET_DEFAULT_MINSIZE,
	                     ALLOCSET_DEFAULT_INITSIZE,
	                     ALLOCSET_DEFAULT_MAXSIZE);
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

		fsstate->td_client = issueQuery(
		                         fdw_option.apikey,
		                         fdw_option.endpoint,
		                         fdw_option.query_engine,
		                         fdw_option.database,
		                         fsstate->query);
	}

	tuple = make_tuple_from_result_row(
	            fsstate->td_client,
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

#if 0
/*
 * treasuredataReScanForeignScan
 *		Restart the scan.
 */
static void
treasuredataReScanForeignScan(ForeignScanState *node)
{
	TdFdwScanState *fsstate = (TdFdwScanState *) node->fdw_state;
	char		sql[64];
	PGresult   *res;

	/* If we haven't created the cursor yet, nothing to do. */
	if (!fsstate->cursor_exists)
		return;

	/*
	 * If any internal parameters affecting this node have changed, we'd
	 * better destroy and recreate the cursor.  Otherwise, rewinding it should
	 * be good enough.  If we've only fetched zero or one batch, we needn't
	 * even rewind the cursor, just rescan what we have.
	 */
	if (node->ss.ps.chgParam != NULL)
	{
		fsstate->cursor_exists = false;
		snprintf(sql, sizeof(sql), "CLOSE c%u",
		         fsstate->cursor_number);
	}
	else if (fsstate->fetch_ct_2 > 1)
	{
		snprintf(sql, sizeof(sql), "MOVE BACKWARD ALL IN c%u",
		         fsstate->cursor_number);
	}
	else
	{
		/* Easy: just rescan what we already have in memory, if anything */
		fsstate->next_tuple = 0;
		return;
	}

	/*
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */
	res = PQexec(fsstate->conn, sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pgfdw_report_error(ERROR, res, fsstate->conn, true, sql);
	PQclear(res);

	/* Now force a fresh FETCH. */
	fsstate->tuples = NULL;
	fsstate->num_tuples = 0;
	fsstate->next_tuple = 0;
	fsstate->fetch_ct_2 = 0;
	fsstate->eof_reached = false;
}
#endif

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

    fsstate->td_client = NULL;

	/* MemoryContexts will be deleted automatically. */
}

#if 0
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
			Form_pg_attribute attr = tupdesc->attrs[attnum - 1];

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
		int			col;

		col = -1;
		while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber	attno = col + FirstLowInvalidHeapAttributeNumber;

			if (attno <= InvalidAttrNumber)		/* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, attno);
		}
	}

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (plan->returningLists)
		returningList = (List *) list_nth(plan->returningLists, subplan_index);

	/*
	 * ON CONFLICT DO UPDATE and DO NOTHING case with inference specification
	 * should have already been rejected in the optimizer, as presently there
	 * is no way to recognize an arbiter index on a foreign table.  Only DO
	 * NOTHING is supported without an inference specification.
	 */
	if (plan->onConflictAction == ONCONFLICT_NOTHING)
		doNothing = true;
	else if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "unexpected ON CONFLICT specification: %d",
		     (int) plan->onConflictAction);

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
	case CMD_UPDATE:
		deparseUpdateSql(&sql, root, resultRelation, rel,
		                 targetAttrs, returningList,
		                 &retrieved_attrs);
		break;
	case CMD_DELETE:
		deparseDeleteSql(&sql, root, resultRelation, rel,
		                 returningList,
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
	RangeTblEntry *rte;
	Oid			userid;
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;
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

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
	rte = rt_fetch(resultRelInfo->ri_RangeTableIndex, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Open connection; report that we'll create a prepared statement. */
	fmstate->conn = GetConnection(server, user, true);
	fmstate->p_name = NULL;		/* prepared statement not made yet */

	/* Deconstruct fdw_private data. */
	fmstate->query = strVal(list_nth(fdw_private,
	                                 FdwModifyPrivateUpdateSql));
	fmstate->target_attrs = (List *) list_nth(fdw_private,
	                        FdwModifyPrivateTargetAttnums);
	fmstate->has_returning = intVal(list_nth(fdw_private,
	                                FdwModifyPrivateHasReturning));
	fmstate->retrieved_attrs = (List *) list_nth(fdw_private,
	                           FdwModifyPrivateRetrievedAttrs);

	/* Create context for per-tuple temp workspace. */
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
	                    "treasuredata_fdw temporary data",
	                    ALLOCSET_SMALL_MINSIZE,
	                    ALLOCSET_SMALL_INITSIZE,
	                    ALLOCSET_SMALL_MAXSIZE);

	/* Prepare for input conversion of RETURNING results. */
	if (fmstate->has_returning)
		fmstate->attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(rel));

	/* Prepare for output conversion of parameters used in prepared stmt. */
	n_params = list_length(fmstate->target_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		/* Find the ctid resjunk column in the subplan's result */
		Plan	   *subplan = mtstate->mt_plans[subplan_index]->plan;

		fmstate->ctidAttno = ExecFindJunkAttributeInTlist(subplan->targetlist,
		                     "ctid");
		if (!AttributeNumberIsValid(fmstate->ctidAttno))
			elog(ERROR, "could not find junk ctid column");

		/* First transmittable parameter will be ctid */
		getTypeOutputInfo(TIDOID, &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}

	if (operation == CMD_INSERT || operation == CMD_UPDATE)
	{
		/* Set up for remaining transmittable parameters */
		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Form_pg_attribute attr = RelationGetDescr(rel)->attrs[attnum - 1];

			Assert(!attr->attisdropped);

			getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
			fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
			fmstate->p_nums++;
		}
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
	const char **p_values;
	PGresult   *res;
	int			n_rows;

	/* Set up the prepared statement on the remote server, if we didn't yet */
	if (!fmstate->p_name)
		prepare_foreign_modify(fmstate);

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate, NULL, slot);

	/*
	 * Execute the prepared statement, and check for success.
	 *
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */
	res = PQexecPrepared(fmstate->conn,
	                     fmstate->p_name,
	                     fmstate->p_nums,
	                     p_values,
	                     NULL,
	                     NULL,
	                     0);
	if (PQresultStatus(res) !=
	        (fmstate->has_returning ? PGRES_TUPLES_OK : PGRES_COMMAND_OK))
		pgfdw_report_error(ERROR, res, fmstate->conn, true, fmstate->query);

	/* Check number of rows affected, and fetch RETURNING tuple if any */
	if (fmstate->has_returning)
	{
		n_rows = PQntuples(res);
		if (n_rows > 0)
			store_returning_result(fmstate, slot, res);
	}
	else
		n_rows = atoi(PQcmdTuples(res));

	/* And clean up */
	PQclear(res);

	MemoryContextReset(fmstate->temp_cxt);

	/* Return NULL if nothing was inserted on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

/*
 * treasuredataExecForeignUpdate
 *		Update one row in a foreign table
 */
static TupleTableSlot *
treasuredataExecForeignUpdate(EState *estate,
                              ResultRelInfo *resultRelInfo,
                              TupleTableSlot *slot,
                              TupleTableSlot *planSlot)
{
	TdFdwModifyState *fmstate = (TdFdwModifyState *) resultRelInfo->ri_FdwState;
	Datum		datum;
	bool		isNull;
	const char **p_values;
	PGresult   *res;
	int			n_rows;

	/* Set up the prepared statement on the remote server, if we didn't yet */
	if (!fmstate->p_name)
		prepare_foreign_modify(fmstate);

	/* Get the ctid that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planSlot,
	                             fmstate->ctidAttno,
	                             &isNull);
	/* shouldn't ever get a null result... */
	if (isNull)
		elog(ERROR, "ctid is NULL");

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate,
	                                    (ItemPointer) DatumGetPointer(datum),
	                                    slot);

	/*
	 * Execute the prepared statement, and check for success.
	 *
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */
	res = PQexecPrepared(fmstate->conn,
	                     fmstate->p_name,
	                     fmstate->p_nums,
	                     p_values,
	                     NULL,
	                     NULL,
	                     0);
	if (PQresultStatus(res) !=
	        (fmstate->has_returning ? PGRES_TUPLES_OK : PGRES_COMMAND_OK))
		pgfdw_report_error(ERROR, res, fmstate->conn, true, fmstate->query);

	/* Check number of rows affected, and fetch RETURNING tuple if any */
	if (fmstate->has_returning)
	{
		n_rows = PQntuples(res);
		if (n_rows > 0)
			store_returning_result(fmstate, slot, res);
	}
	else
		n_rows = atoi(PQcmdTuples(res));

	/* And clean up */
	PQclear(res);

	MemoryContextReset(fmstate->temp_cxt);

	/* Return NULL if nothing was updated on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

/*
 * treasuredataExecForeignDelete
 *		Delete one row from a foreign table
 */
static TupleTableSlot *
treasuredataExecForeignDelete(EState *estate,
                              ResultRelInfo *resultRelInfo,
                              TupleTableSlot *slot,
                              TupleTableSlot *planSlot)
{
	TdFdwModifyState *fmstate = (TdFdwModifyState *) resultRelInfo->ri_FdwState;
	Datum		datum;
	bool		isNull;
	const char **p_values;
	PGresult   *res;
	int			n_rows;

	/* Set up the prepared statement on the remote server, if we didn't yet */
	if (!fmstate->p_name)
		prepare_foreign_modify(fmstate);

	/* Get the ctid that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planSlot,
	                             fmstate->ctidAttno,
	                             &isNull);
	/* shouldn't ever get a null result... */
	if (isNull)
		elog(ERROR, "ctid is NULL");

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate,
	                                    (ItemPointer) DatumGetPointer(datum),
	                                    NULL);

	/*
	 * Execute the prepared statement, and check for success.
	 *
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */
	res = PQexecPrepared(fmstate->conn,
	                     fmstate->p_name,
	                     fmstate->p_nums,
	                     p_values,
	                     NULL,
	                     NULL,
	                     0);
	if (PQresultStatus(res) !=
	        (fmstate->has_returning ? PGRES_TUPLES_OK : PGRES_COMMAND_OK))
		pgfdw_report_error(ERROR, res, fmstate->conn, true, fmstate->query);

	/* Check number of rows affected, and fetch RETURNING tuple if any */
	if (fmstate->has_returning)
	{
		n_rows = PQntuples(res);
		if (n_rows > 0)
			store_returning_result(fmstate, slot, res);
	}
	else
		n_rows = atoi(PQcmdTuples(res));

	/* And clean up */
	PQclear(res);

	MemoryContextReset(fmstate->temp_cxt);

	/* Return NULL if nothing was deleted on the remote end */
	return (n_rows > 0) ? slot : NULL;
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

	/* If we created a prepared statement, destroy it */
	if (fmstate->p_name)
	{
		char		sql[64];
		PGresult   *res;

		snprintf(sql, sizeof(sql), "DEALLOCATE %s", fmstate->p_name);

		/*
		 * We don't use a PG_TRY block here, so be careful not to throw error
		 * without releasing the PGresult.
		 */
		res = PQexec(fmstate->conn, sql);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			pgfdw_report_error(ERROR, res, fmstate->conn, true, sql);
		PQclear(res);
		fmstate->p_name = NULL;
	}

	/* Release remote connection */
	ReleaseConnection(fmstate->conn);
	fmstate->conn = NULL;
}

/*
 * treasuredataIsForeignRelUpdatable
 *		Determine whether a foreign table supports INSERT, UPDATE and/or
 *		DELETE.
 */
static int
treasuredataIsForeignRelUpdatable(Relation rel)
{
	bool		updatable;
	ForeignTable *table;
	ForeignServer *server;
	ListCell   *lc;

	/*
	 * By default, all treasuredata_fdw foreign tables are assumed updatable. This
	 * can be overridden by a per-server setting, which in turn can be
	 * overridden by a per-table setting.
	 */
	updatable = true;

	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "updatable") == 0)
			updatable = defGetBoolean(def);
	}
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "updatable") == 0)
			updatable = defGetBoolean(def);
	}

	/*
	 * Currently "updatable" means support for INSERT, UPDATE and DELETE.
	 */
	return updatable ?
	       (1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE) : 0;
}

/*
 * treasuredataExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void
treasuredataExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	List	   *fdw_private;
	char	   *sql;

	if (es->verbose)
	{
		fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
		ExplainPropertyText("Remote SQL", sql, es);
	}
}

/*
 * treasuredataExplainForeignModify
 *		Produce extra output for EXPLAIN of a ModifyTable on a foreign table
 */
static void
treasuredataExplainForeignModify(ModifyTableState *mtstate,
                                 ResultRelInfo *rinfo,
                                 List *fdw_private,
                                 int subplan_index,
                                 ExplainState *es)
{
	if (es->verbose)
	{
		char	   *sql = strVal(list_nth(fdw_private,
		                                  FdwModifyPrivateUpdateSql));

		ExplainPropertyText("Remote SQL", sql, es);
	}
}
#endif

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
	width = baserel->width;

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

#if 0
/*
 * Detect whether we want to process an EquivalenceClass member.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
static bool
ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
                          EquivalenceClass *ec, EquivalenceMember *em,
                          void *arg)
{
	ec_member_foreign_arg *state = (ec_member_foreign_arg *) arg;
	Expr	   *expr = em->em_expr;

	/*
	 * If we've identified what we're processing in the current scan, we only
	 * want to match that expression.
	 */
	if (state->current != NULL)
		return equal(expr, state->current);

	/*
	 * Otherwise, ignore anything we've already processed.
	 */
	if (list_member(state->already_used, expr))
		return false;

	/* This is the new target to process. */
	state->current = expr;
	return true;
}
#endif

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
int
set_transmission_modes(void)
{
	int			nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void) set_config_option("datestyle", "ISO",
		                         PGC_USERSET, PGC_S_SESSION,
		                         GUC_ACTION_SAVE, true, 0, false);
	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void) set_config_option("intervalstyle", "postgres",
		                         PGC_USERSET, PGC_S_SESSION,
		                         GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void) set_config_option("extra_float_digits", "3",
		                         PGC_USERSET, PGC_S_SESSION,
		                         GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void
reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}

#if 0
/*
 * prepare_foreign_modify
 *		Establish a prepared statement for execution of INSERT/UPDATE/DELETE
 */
static void
prepare_foreign_modify(TdFdwModifyState *fmstate)
{
	char		prep_name[NAMEDATALEN];
	char	   *p_name;
	PGresult   *res;

	/* Construct name we'll use for the prepared statement. */
	snprintf(prep_name, sizeof(prep_name), "pgsql_fdw_prep_%u",
	         GetPrepStmtNumber(fmstate->conn));
	p_name = pstrdup(prep_name);

	/*
	 * We intentionally do not specify parameter types here, but leave the
	 * remote server to derive them by default.  This avoids possible problems
	 * with the remote server using different type OIDs than we do.  All of
	 * the prepared statements we use in this module are simple enough that
	 * the remote server will make the right choices.
	 *
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */
	res = PQprepare(fmstate->conn,
	                p_name,
	                fmstate->query,
	                0,
	                NULL);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pgfdw_report_error(ERROR, res, fmstate->conn, true, fmstate->query);
	PQclear(res);

	/* This action shows that the prepare has been done. */
	fmstate->p_name = p_name;
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
		int			nestlevel;
		ListCell   *lc;

		nestlevel = set_transmission_modes();

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

		reset_transmission_modes(nestlevel);
	}

	Assert(pindex == fmstate->p_nums);

	MemoryContextSwitchTo(oldcontext);

	return p_values;
}

/*
 * store_returning_result
 *		Store the result of a RETURNING clause
 *
 * On error, be sure to release the PGresult on the way out.  Callers do not
 * have PG_TRY blocks to ensure this happens.
 */
static void
store_returning_result(TdFdwModifyState *fmstate,
                       TupleTableSlot *slot, PGresult *res)
{
	PG_TRY();
	{
		HeapTuple	newtup;

		newtup = make_tuple_from_result_row(res, 0,
		                                    fmstate->rel,
		                                    fmstate->attinmeta,
		                                    fmstate->retrieved_attrs,
		                                    fmstate->temp_cxt);
		/* tuple will be deleted when it is cleared from the slot */
		ExecStoreTuple(newtup, slot, InvalidBuffer, true);
	}
	PG_CATCH();
	{
		if (res)
			PQclear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * treasuredataAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
treasuredataAnalyzeForeignTable(Relation relation,
                                AcquireSampleRowsFunc *func,
                                BlockNumber *totalpages)
{
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;
	PGconn	   *conn;
	StringInfoData sql;
	PGresult   *volatile res = NULL;

	/* Return the row-analysis function pointer */
	*func = treasuredataAcquireSampleRowsFunc;

	/*
	 * Now we have to get the number of pages.  It's annoying that the ANALYZE
	 * API requires us to return that now, because it forces some duplication
	 * of effort between this routine and treasuredataAcquireSampleRowsFunc.  But
	 * it's probably not worth redefining that API at this point.
	 */

	/*
	 * Get the connection to use.  We do the remote access as the table's
	 * owner, even if the ANALYZE was started by some other user.
	 */
	table = GetForeignTable(RelationGetRelid(relation));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(relation->rd_rel->relowner, server->serverid);
	conn = GetConnection(server, user, false);

	/*
	 * Construct command to get page count for relation.
	 */
	initStringInfo(&sql);
	deparseAnalyzeSizeSql(&sql, relation);

	/* In what follows, do not risk leaking any PGresults. */
	PG_TRY();
	{
		res = PQexec(conn, sql.data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			pgfdw_report_error(ERROR, res, conn, false, sql.data);

		if (PQntuples(res) != 1 || PQnfields(res) != 1)
			elog(ERROR, "unexpected result from deparseAnalyzeSizeSql query");
		*totalpages = strtoul(PQgetvalue(res, 0, 0), NULL, 10);

		PQclear(res);
		res = NULL;
	}
	PG_CATCH();
	{
		if (res)
			PQclear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleaseConnection(conn);

	return true;
}

/*
 * Acquire a random sample of rows from foreign table managed by treasuredata_fdw.
 *
 * We fetch the whole table from the remote side and pick out some sample rows.
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the table and return it into
 * *totalrows.  Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the table.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
static int
treasuredataAcquireSampleRowsFunc(Relation relation, int elevel,
                                  HeapTuple *rows, int targrows,
                                  double *totalrows,
                                  double *totaldeadrows)
{
	TdFdwAnalyzeState astate;
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;
	PGconn	   *conn;
	unsigned int cursor_number;
	StringInfoData sql;
	PGresult   *volatile res = NULL;

	/* Initialize workspace state */
	astate.rel = relation;
	astate.attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(relation));

	astate.rows = rows;
	astate.targrows = targrows;
	astate.numrows = 0;
	astate.samplerows = 0;
	astate.rowstoskip = -1;		/* -1 means not set yet */
	reservoir_init_selection_state(&astate.rstate, targrows);

	/* Remember ANALYZE context, and create a per-tuple temp context */
	astate.anl_cxt = CurrentMemoryContext;
	astate.temp_cxt = AllocSetContextCreate(CurrentMemoryContext,
	                                        "treasuredata_fdw temporary data",
	                                        ALLOCSET_SMALL_MINSIZE,
	                                        ALLOCSET_SMALL_INITSIZE,
	                                        ALLOCSET_SMALL_MAXSIZE);

	/*
	 * Get the connection to use.  We do the remote access as the table's
	 * owner, even if the ANALYZE was started by some other user.
	 */
	table = GetForeignTable(RelationGetRelid(relation));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(relation->rd_rel->relowner, server->serverid);
	conn = GetConnection(server, user, false);

	/*
	 * Construct cursor that retrieves whole rows from remote.
	 */
	cursor_number = GetCursorNumber(conn);
	initStringInfo(&sql);
	appendStringInfo(&sql, "DECLARE c%u CURSOR FOR ", cursor_number);
	deparseAnalyzeSql(&sql, relation, &astate.retrieved_attrs);

	/* In what follows, do not risk leaking any PGresults. */
	PG_TRY();
	{
		res = PQexec(conn, sql.data);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			pgfdw_report_error(ERROR, res, conn, false, sql.data);
		PQclear(res);
		res = NULL;

		/* Retrieve and process rows a batch at a time. */
		for (;;)
		{
			char		fetch_sql[64];
			int			fetch_size;
			int			numrows;
			int			i;

			/* Allow users to cancel long query */
			CHECK_FOR_INTERRUPTS();

			/*
			 * XXX possible future improvement: if rowstoskip is large, we
			 * could issue a MOVE rather than physically fetching the rows,
			 * then just adjust rowstoskip and samplerows appropriately.
			 */

			/* The fetch size is arbitrary, but shouldn't be enormous. */
			fetch_size = 100;

			/* Fetch some rows */
			snprintf(fetch_sql, sizeof(fetch_sql), "FETCH %d FROM c%u",
			         fetch_size, cursor_number);

			res = PQexec(conn, fetch_sql);
			/* On error, report the original query, not the FETCH. */
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
				pgfdw_report_error(ERROR, res, conn, false, sql.data);

			/* Process whatever we got. */
			numrows = PQntuples(res);
			for (i = 0; i < numrows; i++)
				analyze_row_processor(res, i, &astate);

			PQclear(res);
			res = NULL;

			/* Must be EOF if we didn't get all the rows requested. */
			if (numrows < fetch_size)
				break;
		}

		/* Close the cursor, just to be tidy. */
		close_cursor(conn, cursor_number);
	}
	PG_CATCH();
	{
		if (res)
			PQclear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleaseConnection(conn);

	/* We assume that we have no dead tuple. */
	*totaldeadrows = 0.0;

	/* We've retrieved all living tuples from foreign server. */
	*totalrows = astate.samplerows;

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
	        (errmsg("\"%s\": table contains %.0f rows, %d rows in sample",
	                RelationGetRelationName(relation),
	                astate.samplerows, astate.numrows)));

	return astate.numrows;
}

/*
 * Collect sample rows from the result of query.
 *	 - Use all tuples in sample until target # of samples are collected.
 *	 - Subsequently, replace already-sampled tuples randomly.
 */
static void
analyze_row_processor(PGresult *res, int row, TdFdwAnalyzeState *astate)
{
	int			targrows = astate->targrows;
	int			pos;			/* array index to store tuple in */
	MemoryContext oldcontext;

	/* Always increment sample row counter. */
	astate->samplerows += 1;

	/*
	 * Determine the slot where this sample row should be stored.  Set pos to
	 * negative value to indicate the row should be skipped.
	 */
	if (astate->numrows < targrows)
	{
		/* First targrows rows are always included into the sample */
		pos = astate->numrows++;
	}
	else
	{
		/*
		 * Now we start replacing tuples in the sample until we reach the end
		 * of the relation.  Same algorithm as in acquire_sample_rows in
		 * analyze.c; see Jeff Vitter's paper.
		 */
		if (astate->rowstoskip < 0)
			astate->rowstoskip = reservoir_get_next_S(&astate->rstate, astate->samplerows, targrows);

		if (astate->rowstoskip <= 0)
		{
			/* Choose a random reservoir element to replace. */
			pos = (int) (targrows * sampler_random_fract(astate->rstate.randstate));
			Assert(pos >= 0 && pos < targrows);
			heap_freetuple(astate->rows[pos]);
		}
		else
		{
			/* Skip this tuple. */
			pos = -1;
		}

		astate->rowstoskip -= 1;
	}

	if (pos >= 0)
	{
		/*
		 * Create sample tuple from current result row, and store it in the
		 * position determined above.  The tuple has to be created in anl_cxt.
		 */
		oldcontext = MemoryContextSwitchTo(astate->anl_cxt);

		astate->rows[pos] = make_tuple_from_result_row(res, row,
		                    astate->rel,
		                    astate->attinmeta,
		                    astate->retrieved_attrs,
		                    astate->temp_cxt);

		MemoryContextSwitchTo(oldcontext);
	}
}

/*
 * Import a foreign schema
 */
static List *
treasuredataImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	List	   *commands = NIL;
	bool		import_collate = true;
	bool		import_default = false;
	bool		import_not_null = true;
	ForeignServer *server;
	UserMapping *mapping;
	PGconn	   *conn;
	StringInfoData buf;
	PGresult   *volatile res = NULL;
	int			numrows,
	            i;
	ListCell   *lc;

	/* Parse statement options */
	foreach(lc, stmt->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "import_collate") == 0)
			import_collate = defGetBoolean(def);
		else if (strcmp(def->defname, "import_default") == 0)
			import_default = defGetBoolean(def);
		else if (strcmp(def->defname, "import_not_null") == 0)
			import_not_null = defGetBoolean(def);
		else
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			         errmsg("invalid option \"%s\"", def->defname)));
	}

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	server = GetForeignServer(serverOid);
	mapping = GetUserMapping(GetUserId(), server->serverid);
	conn = GetConnection(server, mapping, false);

	/* Don't attempt to import collation if remote server hasn't got it */
	if (PQserverVersion(conn) < 90100)
		import_collate = false;

	/* Create workspace for strings */
	initStringInfo(&buf);

	/* In what follows, do not risk leaking any PGresults. */
	PG_TRY();
	{
		/* Check that the schema really exists */
		appendStringInfoString(&buf, "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = ");
		deparseStringLiteral(&buf, stmt->remote_schema);

		res = PQexec(conn, buf.data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			pgfdw_report_error(ERROR, res, conn, false, buf.data);

		if (PQntuples(res) != 1)
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND),
			         errmsg("schema \"%s\" is not present on foreign server \"%s\"",
			                stmt->remote_schema, server->servername)));

		PQclear(res);
		res = NULL;
		resetStringInfo(&buf);

		/*
		 * Fetch all table data from this schema, possibly restricted by
		 * EXCEPT or LIMIT TO.  (We don't actually need to pay any attention
		 * to EXCEPT/LIMIT TO here, because the core code will filter the
		 * statements we return according to those lists anyway.  But it
		 * should save a few cycles to not process excluded tables in the
		 * first place.)
		 *
		 * Note: because we run the connection with search_path restricted to
		 * pg_catalog, the format_type() and pg_get_expr() outputs will always
		 * include a schema name for types/functions in other schemas, which
		 * is what we want.
		 */
		if (import_collate)
			appendStringInfoString(&buf,
			                       "SELECT relname, "
			                       "  attname, "
			                       "  format_type(atttypid, atttypmod), "
			                       "  attnotnull, "
			                       "  pg_get_expr(adbin, adrelid), "
			                       "  collname, "
			                       "  collnsp.nspname "
			                       "FROM pg_class c "
			                       "  JOIN pg_namespace n ON "
			                       "    relnamespace = n.oid "
			                       "  LEFT JOIN pg_attribute a ON "
			                       "    attrelid = c.oid AND attnum > 0 "
			                       "      AND NOT attisdropped "
			                       "  LEFT JOIN pg_attrdef ad ON "
			                       "    adrelid = c.oid AND adnum = attnum "
			                       "  LEFT JOIN pg_collation coll ON "
			                       "    coll.oid = attcollation "
			                       "  LEFT JOIN pg_namespace collnsp ON "
			                       "    collnsp.oid = collnamespace ");
		else
			appendStringInfoString(&buf,
			                       "SELECT relname, "
			                       "  attname, "
			                       "  format_type(atttypid, atttypmod), "
			                       "  attnotnull, "
			                       "  pg_get_expr(adbin, adrelid), "
			                       "  NULL, NULL "
			                       "FROM pg_class c "
			                       "  JOIN pg_namespace n ON "
			                       "    relnamespace = n.oid "
			                       "  LEFT JOIN pg_attribute a ON "
			                       "    attrelid = c.oid AND attnum > 0 "
			                       "      AND NOT attisdropped "
			                       "  LEFT JOIN pg_attrdef ad ON "
			                       "    adrelid = c.oid AND adnum = attnum ");

		appendStringInfoString(&buf,
		                       "WHERE c.relkind IN ('r', 'v', 'f', 'm') "
		                       "  AND n.nspname = ");
		deparseStringLiteral(&buf, stmt->remote_schema);

		/* Apply restrictions for LIMIT TO and EXCEPT */
		if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO ||
		        stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
		{
			bool		first_item = true;

			appendStringInfoString(&buf, " AND c.relname ");
			if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
				appendStringInfoString(&buf, "NOT ");
			appendStringInfoString(&buf, "IN (");

			/* Append list of table names within IN clause */
			foreach(lc, stmt->table_list)
			{
				RangeVar   *rv = (RangeVar *) lfirst(lc);

				if (first_item)
					first_item = false;
				else
					appendStringInfoString(&buf, ", ");
				deparseStringLiteral(&buf, rv->relname);
			}
			appendStringInfoChar(&buf, ')');
		}

		/* Append ORDER BY at the end of query to ensure output ordering */
		appendStringInfoString(&buf, " ORDER BY c.relname, a.attnum");

		/* Fetch the data */
		res = PQexec(conn, buf.data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			pgfdw_report_error(ERROR, res, conn, false, buf.data);

		/* Process results */
		numrows = PQntuples(res);
		/* note: incrementation of i happens in inner loop's while() test */
		for (i = 0; i < numrows;)
		{
			char	   *tablename = PQgetvalue(res, i, 0);
			bool		first_item = true;

			resetStringInfo(&buf);
			appendStringInfo(&buf, "CREATE FOREIGN TABLE %s (\n",
			                 quote_identifier(tablename));

			/* Scan all rows for this table */
			do
			{
				char	   *attname;
				char	   *typename;
				char	   *attnotnull;
				char	   *attdefault;
				char	   *collname;
				char	   *collnamespace;

				/* If table has no columns, we'll see nulls here */
				if (PQgetisnull(res, i, 1))
					continue;

				attname = PQgetvalue(res, i, 1);
				typename = PQgetvalue(res, i, 2);
				attnotnull = PQgetvalue(res, i, 3);
				attdefault = PQgetisnull(res, i, 4) ? (char *) NULL :
				             PQgetvalue(res, i, 4);
				collname = PQgetisnull(res, i, 5) ? (char *) NULL :
				           PQgetvalue(res, i, 5);
				collnamespace = PQgetisnull(res, i, 6) ? (char *) NULL :
				                PQgetvalue(res, i, 6);

				if (first_item)
					first_item = false;
				else
					appendStringInfoString(&buf, ",\n");

				/* Print column name and type */
				appendStringInfo(&buf, "  %s %s",
				                 quote_identifier(attname),
				                 typename);

				/*
				 * Add column_name option so that renaming the foreign table's
				 * column doesn't break the association to the underlying
				 * column.
				 */
				appendStringInfoString(&buf, " OPTIONS (column_name ");
				deparseStringLiteral(&buf, attname);
				appendStringInfoChar(&buf, ')');

				/* Add COLLATE if needed */
				if (import_collate && collname != NULL && collnamespace != NULL)
					appendStringInfo(&buf, " COLLATE %s.%s",
					                 quote_identifier(collnamespace),
					                 quote_identifier(collname));

				/* Add DEFAULT if needed */
				if (import_default && attdefault != NULL)
					appendStringInfo(&buf, " DEFAULT %s", attdefault);

				/* Add NOT NULL if needed */
				if (import_not_null && attnotnull[0] == 't')
					appendStringInfoString(&buf, " NOT NULL");
			}
			while (++i < numrows &&
			        strcmp(PQgetvalue(res, i, 0), tablename) == 0);

			/*
			 * Add server name and table-level options.  We specify remote
			 * schema and table name as options (the latter to ensure that
			 * renaming the foreign table doesn't break the association).
			 */
			appendStringInfo(&buf, "\n) SERVER %s\nOPTIONS (",
			                 quote_identifier(server->servername));

			appendStringInfoString(&buf, "schema_name ");
			deparseStringLiteral(&buf, stmt->remote_schema);
			appendStringInfoString(&buf, ", table_name ");
			deparseStringLiteral(&buf, tablename);

			appendStringInfoString(&buf, ");");

			commands = lappend(commands, pstrdup(buf.data));
		}

		/* Clean up */
		PQclear(res);
		res = NULL;
	}
	PG_CATCH();
	{
		if (res)
			PQclear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleaseConnection(conn);

	return commands;
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
		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(temp_context);
		return NULL;
	}

	/*
	 * i indexes columns in the relation, j indexes columns in the PGresult.
	 */
	j = 0;
	foreach(lc, retrieved_attrs)
	{
		int			i = lfirst_int(lc);
		char	   *valstr;

		valstr = result_values[j];

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
		           NameStr(tupdesc->attrs[errpos->cur_attno - 1]->attname),
		           RelationGetRelationName(errpos->rel));
}

