/*
 * The Treasure Data Foreign Data Wrapper allows you to fetch foreign data
 * in your PostgreSQL server
 *
 * This software is released under the postgresql licence
 *
 * author: Mitsunori Komatsu
 */
#include "postgres.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "parser/parsetree.h"
#include "treasuredata_fdw.h"
#include "bridge.h"

PG_MODULE_MAGIC;

extern Datum treasuredata_fdw_handler(PG_FUNCTION_ARGS);
extern Datum treasuredata_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(treasuredata_fdw_handler);
PG_FUNCTION_INFO_V1(treasuredata_fdw_validator);

struct Option
{
	const char *optname;
	Oid optcontext;             /* Oid of catalog in which option may appear */
};

static const struct Option valid_options[] =
{
	{"endpoint", ForeignTableRelationId},
	{"query_engine", ForeignTableRelationId},
	{"apikey", ForeignTableRelationId},
	{"database", ForeignTableRelationId},
	{"table", ForeignTableRelationId},
	{"query", ForeignTableRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};


void _PG_init(void);
void _PG_fini(void);

/*
 * FDW functions declarations
 */

static void tdGetForeignRelSize(PlannerInfo *root,
                                RelOptInfo *baserel,
                                Oid foreigntableid);
static void tdGetForeignPaths(PlannerInfo *root,
                              RelOptInfo *baserel,
                              Oid foreigntableid);
static ForeignScan *tdGetForeignPlan(PlannerInfo *root,
                                     RelOptInfo *baserel,
                                     Oid foreigntableid,
                                     ForeignPath *best_path,
                                     List *tlist,
                                     List *scan_clauses
#if PG_VERSION_NUM >= 90500
                                     , Plan *outer_plan
#endif
                                    );
static void tdExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void tdBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *tdIterateForeignScan(ForeignScanState *node);
static void tdReScanForeignScan(ForeignScanState *node);
static void tdEndForeignScan(ForeignScanState *node);

/*	Helpers functions */
static void *serializePlanState(TdFdwPlanState * planstate);
static TdFdwExecState *initializeExecState(void *internal_plan_state);
static void tdGetOptions(Oid foreigntableid, TdFdwOption *fdw_option);
static void * executeQuery(TdFdwOption *fdw_option, List *target_list);
static int resultToTuple(TdFdwExecState *execstate, TupleTableSlot *slot);

Datum
treasuredata_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdw_routine = makeNode(FdwRoutine);

	/* Plan phase */
	fdw_routine->GetForeignRelSize = tdGetForeignRelSize;
	fdw_routine->GetForeignPaths = tdGetForeignPaths;
	fdw_routine->GetForeignPlan = tdGetForeignPlan;
	fdw_routine->ExplainForeignScan = tdExplainForeignScan;

	/* Scan phase */
	fdw_routine->BeginForeignScan = tdBeginForeignScan;
	fdw_routine->IterateForeignScan = tdIterateForeignScan;
	fdw_routine->ReScanForeignScan = tdReScanForeignScan;
	fdw_routine->EndForeignScan = tdEndForeignScan;

	PG_RETURN_POINTER(fdw_routine);
}

/*
 * Check if the option is valid.
 */
static bool
validate_option(DefElem *def, Oid context)
{
	const struct Option *opt;
	bool is_valid = false;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, def->defname) == 0)
			is_valid = true;
	}

	if (!is_valid)
	{
		StringInfoData buf;

		/*
		 * Unknown option specified, complain about it. Provide a hint
		 * with list of valid options for the object.
		 */
		initStringInfo(&buf);
		for (opt = valid_options; opt->optname; opt++)
		{
			if (context == opt->optcontext)
				appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
				                 opt->optname);
		}

		ereport(ERROR,
		        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
		         errmsg("invalid option \"%s\"", def->defname),
		         buf.len > 0
		         ? errhint("Valid options in this context are: %s",
		                   buf.data)
		         : errhint("There are no valid options in this context.")));
	}

	return is_valid;
}

Datum
treasuredata_fdw_validator(PG_FUNCTION_ARGS)
{
	List       *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid        catalog = PG_GETARG_OID(1);
	char       *endpoint = NULL;
	char       *query_engine = NULL;
	char       *apikey = NULL;
	char       *database = NULL;
	char       *table = NULL;
	char       *query = NULL;
	ListCell   *cell;

	/*
	 * Only superusers are allowed to set options of a file_fdw foreign table.
	 * This is because the filename is one of those options, and we don't want
	 * non-superusers to be able to determine which file gets read.
	 *
	 * Putting this sort of permissions check in a validator is a bit of a
	 * crock, but there doesn't seem to be any other place that can enforce
	 * the check more cleanly.
	 *
	 * Note that the valid_options[] array disallows setting filename at any
	 * options level other than foreign table --- otherwise there'd still be a
	 * security hole.
	 */
	if (catalog == ForeignTableRelationId && !superuser())
		ereport(ERROR,
		        (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
		         errmsg("only superuser can change options of a treasuredata_fdw foreign table")));

	/*
	 * Check that only options supported by treasuredata_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		/*
		 * Check if the option is valid with looking up Option.
		 */
		validate_option(def, catalog);

		if (strcmp(def->defname, "endpoint") == 0)
		{
			if (endpoint)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));

			/*
			 * Option value can be obtained by useing defGetXXX() function:
			 * typically defGetString(), defGetNumeric(), defGetBoolean() or
			 * defGetInt64().
			 *
			 * See commands/defrem.h for more information about defGetXXX()
			 * functions.
			 */
			endpoint = defGetString(def);
		}
		else if (strcmp(def->defname, "query_engine") == 0)
		{
			if (query_engine)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));

			query_engine = defGetString(def);
		}
		else if (strcmp(def->defname, "apikey") == 0)
		{
			if (apikey)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));

			apikey = defGetString(def);
		}
		else if (strcmp(def->defname, "database") == 0)
		{
			if (database)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));

			database = defGetString(def);
		}
		else if (strcmp(def->defname, "table") == 0)
		{
			if (table)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));

			table = defGetString(def);
		}
		else if (strcmp(def->defname, "query") == 0)
		{
			if (query)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));

			query = defGetString(def);
		}
	}

	if (catalog == ForeignTableRelationId)
	{
		if (query_engine == NULL)
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			         errmsg("query_engine is required for treasuredata_fdw foreign tables")));

		if (apikey == NULL)
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			         errmsg("apikey is required for treasuredata_fdw foreign tables")));

		if (database == NULL)
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			         errmsg("database is required for treasuredata_fdw foreign tables")));

		if (table == NULL && query == NULL)
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			         errmsg("table or query is required for treasuredata_fdw foreign tables")));
	}

	PG_RETURN_VOID();
}

/*
 * tdGetForeignRelSize
 *		Obtain relation size estimates for a foreign table.
 *		This is done by calling the
 */
static void
tdGetForeignRelSize(PlannerInfo *root,
                    RelOptInfo *baserel,
                    Oid foreigntableid)
{
	TdFdwPlanState *planstate = palloc0(sizeof(TdFdwPlanState));
	ForeignTable *ftable = GetForeignTable(foreigntableid);
	ListCell   *lc;
	bool		needWholeRow = false;
	TupleDesc	desc;

	baserel->fdw_private = planstate;
	planstate->foreigntableid = foreigntableid;
	/* Initialize the conversion info array */
	{
		Relation	rel = RelationIdGetRelation(ftable->relid);
		AttInMetadata *attinmeta;

		desc = RelationGetDescr(rel);
		attinmeta = TupleDescGetAttInMetadata(desc);
		planstate->numattrs = RelationGetNumberOfAttributes(rel);

		planstate->cinfos = palloc0(sizeof(ConversionInfo *) * planstate->numattrs);
		initConversioninfo(planstate->cinfos, attinmeta);
		needWholeRow = rel->trigdesc && rel->trigdesc->trig_insert_after_row;
		RelationClose(rel);
	}

	if (needWholeRow)
	{
		int	i;

		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = desc->attrs[i];

			if (!att->attisdropped)
			{
				planstate->target_list = lappend(planstate->target_list, makeString(NameStr(att->attname)));
			}
		}
	}
	else
	{
		/* Pull "var" clauses to build an appropriate target list */
#if PG_VERSION_NUM >= 90600
		foreach(lc, extractColumns(baserel->reltarget->exprs, baserel->baserestrictinfo))
#else
		foreach(lc, extractColumns(baserel->reltargetlist, baserel->baserestrictinfo))
#endif
		{
			Var		   *var = (Var *) lfirst(lc);
			Value	   *colname;

			/*
			 * Store only a Value node containing the string name of the
			 * column.
			 */
			colname = colnameFromVar(var, root, planstate);
			if (colname != NULL && strVal(colname) != NULL)
			{
				planstate->target_list = lappend(planstate->target_list, colname);
			}
		}
	}
	/* Extract the restrictions from the plan. */
	foreach(lc, baserel->baserestrictinfo)
	{
		extractRestrictions(baserel->relids, ((RestrictInfo *) lfirst(lc))->clause,
		                    &planstate->qual_list);

	}

	/* TODO: Revisit this value */
	baserel->rows = 10000;
	baserel->tuples = 10000;
}

static void
tdGetForeignPaths(PlannerInfo *root,
                  RelOptInfo *baserel,
                  Oid foreigntableid)
{

	add_path(baserel, (Path *)
	         create_foreignscan_path(root, baserel,
	                                 baserel->rows,
	                                 0,  /* startup cost */
	                                 0,  /* total cost */
	                                 NIL,    /* no pathkeys */
	                                 NULL,   /* no outer rel either */
#if PG_VERSION_NUM >= 90500
	                                 NULL,   /* no extra plan */
#endif
	                                 NULL)); /* no fdw_private data */
}

/*
 * tdGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
tdGetForeignPlan(PlannerInfo *root,
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
	Index		scan_relid = baserel->relid;
	TdFdwPlanState *planstate = (TdFdwPlanState *) baserel->fdw_private;
	ListCell   *lc;

	scan_clauses = extract_actual_clauses(scan_clauses, false);
	/* Extract the quals coming from a parameterized path, if any */
	if (best_path->path.param_info)
	{
		foreach(lc, scan_clauses)
		{
			extractRestrictions(baserel->relids, (Expr *) lfirst(lc),
			                    &planstate->qual_list);
		}
	}
	planstate->pathkeys = (List *) best_path->fdw_private;
	return make_foreignscan(tlist,
	                        scan_clauses,
	                        scan_relid,
	                        scan_clauses,		/* no expressions to evaluate */
	                        serializePlanState(planstate)
#if PG_VERSION_NUM >= 90500
	                        , NULL
	                        , NULL /* All quals are meant to be rechecked */
	                        , NULL
#endif
	                       );
}

/*
 * tdExplainForeignScan
 *		Placeholder for additional "EXPLAIN" information.
 *		as information that was taken into account for the choice of a path.
 */
static void
tdExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
}

static void
createSelectStmt(StringInfoData *sql, TdFdwOption *fdw_option, List *target_list)
{
	ListCell   *lc;
	bool is_first = true;

	if (fdw_option->query == NULL)
	{
		appendStringInfo(sql, "SELECT ");
		foreach(lc, target_list)
		{
			if (is_first)
			{
				is_first = false;
			}
			else
			{
				appendStringInfo(sql, ", ");
			}
			appendStringInfo(sql, "%s", ((Value *)lfirst(lc))->val.str);
		}
		appendStringInfo(sql, " FROM %s", fdw_option->table);
	}
	else
	{
		// FIXME
	}
}

static void *
executeQuery(TdFdwOption *fdw_option, List *target_list)
{
	StringInfoData sql;
	initStringInfo(&sql);
	createSelectStmt(&sql, fdw_option, target_list);

	elog(DEBUG1, "treasuredata_fdw: endpoint=%s, query_engine=%s, apikey=%s, database=%s, table=%s, query=%s",
	     fdw_option->endpoint,
	     fdw_option->query_engine,
	     fdw_option->apikey,
	     fdw_option->database,
	     fdw_option->table,
	     sql.data);

	// Issue a query to Treasure Data
	return issueQuery(
	           fdw_option->apikey,
	           fdw_option->endpoint,
	           fdw_option->query_engine,
	           fdw_option->database,
	           sql.data);
}

/*
 *	tdBeginForeignScan
 *		Initialize the foreign scan.
 *		This (primarily) involves :
 *			- retrieving cached info from the plan phase
 *			- initializing various buffers
 */
static void
tdBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fscan = (ForeignScan *) node->ss.ps.plan;
	TdFdwExecState *execstate;
	TupleDesc	tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
	ListCell   *lc;

	execstate = initializeExecState(fscan->fdw_private);
	execstate->values = palloc(sizeof(Datum) * tupdesc->natts);
	execstate->nulls = palloc(sizeof(bool) * tupdesc->natts);
	execstate->scaned_column_indexes = palloc(sizeof(int) * tupdesc->natts);
	execstate->qual_list = NULL;
	foreach(lc, fscan->fdw_exprs)
	{
		extractRestrictions(bms_make_singleton(fscan->scan.scanrelid),
		                    ((Expr *) lfirst(lc)),
		                    &execstate->qual_list);
	}
	initConversioninfo(execstate->cinfos, TupleDescGetAttInMetadata(tupdesc));

	node->fdw_state = execstate;

	{
		int i, j;
		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = tupdesc->attrs[i];

			execstate->scaned_column_indexes[i] = -1;

			j = 0;
			foreach(lc, execstate->target_list)
			{
				if (strcmp(NameStr(attr->attname), ((Value *)lfirst(lc))->val.str) == 0)
				{
					execstate->scaned_column_indexes[i] = j;
				}
				j++;
			}

		}
	}
}


/*
 * tdIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
static TupleTableSlot *
tdIterateForeignScan(ForeignScanState *node)
{
	TdFdwExecState *execstate = (TdFdwExecState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (execstate->td_client == NULL)
	{
		TdFdwOption fdw_option;
		tdGetOptions(execstate->foreigntableid, &fdw_option);
		if ((execstate->td_client = executeQuery(&fdw_option, execstate->target_list)) == NULL)
		{
			elog(ERROR, "treasuredata_fdw: tdIterateForeignScan: Failed to execute the query. \
                          endpoint=%s, query_engine=%s, apikey=%s, database=%s, table=%s, query=%s",
			     fdw_option.endpoint,
			     fdw_option.query_engine,
			     fdw_option.apikey,
			     fdw_option.database,
			     fdw_option.table,
			     fdw_option.query);
		}
	}

	ExecClearTuple(slot);

	if (resultToTuple(execstate, slot) == 0)
	{
		ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * tdReScanForeignScan
 *		Restart the scan
 */
static void
tdReScanForeignScan(ForeignScanState *node)
{
}

/*
 *	tdEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan.
 */
static void
tdEndForeignScan(ForeignScanState *node)
{
}

/*
 *	"Serialize" a TdFdwPlanState, so that it is safe to be carried
 *	between the plan and the execution safe.
 */
static void *
serializePlanState(TdFdwPlanState * state)
{
	List	   *result = NULL;

	result = lappend(result, makeConst(INT4OID,
	                                   -1, InvalidOid, 4, Int32GetDatum(state->numattrs), false, true));
	result = lappend(result, makeConst(INT4OID,
	                                   -1, InvalidOid, 4, Int32GetDatum(state->foreigntableid), false, true));
	result = lappend(result, state->target_list);

	result = lappend(result, serializeDeparsedSortGroup(state->pathkeys));

	return result;
}

/*
 *	"Deserialize" an internal state and inject it in an
 *	TdFdwExecState
 */
static TdFdwExecState *
initializeExecState(void *internalstate)
{
	TdFdwExecState *execstate = palloc0(sizeof(TdFdwExecState));
	List	   *values = (List *) internalstate;
	AttrNumber	attnum = ((Const *) linitial(values))->constvalue;
	Oid			foreigntableid = ((Const *) lsecond(values))->constvalue;
	List		*pathkeys;

	/* Those list must be copied, because their memory context can become */
	/* invalid during the execution (in particular with the cursor interface) */
	execstate->foreigntableid = foreigntableid;
	execstate->target_list = copyObject(lthird(values));
	pathkeys = lfourth(values);
	execstate->pathkeys = deserializeDeparsedSortGroup(pathkeys);
	execstate->buffer = makeStringInfo();
	execstate->cinfos = palloc0(sizeof(ConversionInfo *) * attnum);
	execstate->values = palloc(attnum * sizeof(Datum));
	execstate->nulls = palloc(attnum * sizeof(bool));
	return execstate;
}

static void
tdGetOptions(Oid foreigntableid, TdFdwOption *fdw_option)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;

	List	   *options;
	ListCell   *cell, *prev;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	fdw_option->endpoint = NULL;
	fdw_option->query_engine = NULL;
	fdw_option->apikey = NULL;
	fdw_option->database = NULL;
	fdw_option->table = NULL;
	fdw_option->query = NULL;

retry:
	prev = NULL;
	foreach(cell, options)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "endpoint") == 0)
		{
			fdw_option->endpoint = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "query_engine") == 0)
		{
			fdw_option->query_engine = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "apikey") == 0)
		{
			fdw_option->apikey = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "database") == 0)
		{
			fdw_option->database = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "table") == 0)
		{
			fdw_option->table = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "query") == 0)
		{
			fdw_option->query = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else
			prev = cell;
	}

	/*
	 * Check required option(s) here.
	 */
	if (fdw_option->query_engine == NULL)
	{
		elog(ERROR, "treasuredata_fdw: query_engine is required for treasuredata_fdw foreign tables");
	}
	if (fdw_option->apikey == NULL)
	{
		elog(ERROR, "treasuredata_fdw: apikey is required for treasuredata_fdw foreign tables");
	}
	if (fdw_option->database == NULL)
	{
		elog(ERROR, "treasuredata_fdw: database is required for treasuredata_fdw foreign tables");
	}
	if (fdw_option->table == NULL && fdw_option->query == NULL)
	{
		elog(ERROR, "treasuredata_fdw: table or query is required for treasuredata_fdw foreign tables");
	}

	elog(DEBUG1, "treasuredata_fdw: endpoint=%s, query_engine=%s, apikey=%s, database=%s, table=%s, query=%s",
	     fdw_option->endpoint,
	     fdw_option->query_engine,
	     fdw_option->apikey,
	     fdw_option->database,
	     fdw_option->table,
	     fdw_option->query);
}

static int
resultToTuple(TdFdwExecState *execstate, TupleTableSlot *slot)
{
	int i;
	int ret;
	TupleDesc tupdesc = slot->tts_tupleDescriptor;
	Datum *values = slot->tts_values;
	bool  *nulls = slot->tts_isnull;
	ConversionInfo **cinfos = execstate->cinfos;
	int *scaned_column_indexes = execstate->scaned_column_indexes;
	char **result_values = (char **)palloc(sizeof(char *) * tupdesc->natts);

	MemSet(values, 0, tupdesc->natts * sizeof(Datum));
	MemSet(nulls, true, tupdesc->natts * sizeof(bool));

	ret = fetchResultRow(execstate->td_client, tupdesc->natts, result_values);

	if (ret == 0)
	{
		for (i = 0; i < tupdesc->natts; i++)
		{
			Datum value = 0;
			char *result_value;
			Form_pg_attribute attr = tupdesc->attrs[i];
			AttrNumber  cinfo_idx = attr->attnum - 1;
			ConversionInfo *cinfo = cinfos[cinfo_idx];

			elog(DEBUG5, "i=%d, attnum=%d, attrname=%s, scaned_column_indexes=%d",
			     i, attr->attnum, NameStr(attr->attname), scaned_column_indexes[cinfo_idx]);

			if (cinfo == NULL || scaned_column_indexes[cinfo_idx] < 0)
			{
				continue;
			}

			result_value = result_values[scaned_column_indexes[cinfo_idx]];
			if (result_value == NULL)
			{
				continue;
			}

			if (cinfo->atttypoid == BYTEAOID || cinfo->atttypoid == TEXTOID ||
			        cinfo->atttypoid == VARCHAROID)
			{
				elog(DEBUG5, "Calling PointerGetDatum()");

				/*
				 * Special case, since the value is already a byte string.
				 */
				value = PointerGetDatum(cstring_to_text_with_len(result_value,
				                        strlen(result_value)));
			}
			else
			{
				elog(DEBUG5, "Calling InputFunctionCall()");

				value = InputFunctionCall(cinfo->attinfunc,
				                          result_value,
				                          cinfo->attioparam,
				                          cinfo->atttypmod);
			}
			values[i] = value;
			nulls[i] = false;
		}
	}

	pfree(result_values);

	return ret;
}
