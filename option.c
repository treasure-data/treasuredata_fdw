/*-------------------------------------------------------------------------
 *
 * option.c
 *		  FDW option handling for treasuredata_fdw
 *
 * Portions Copyright (c) 2016, Mitsunori Komatsu
 *
 * IDENTIFICATION
 *		  option.c
 *
 *-------------------------------------------------------------------------
 */
#include "server/postgres.h"

#include "treasuredata_fdw.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "utils/builtins.h"

extern Datum treasuredata_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(treasuredata_fdw_validator);

/*
 * Describes the valid options for objects that this wrapper uses.
 */
typedef struct PgFdwOption
{
	const char *keyword;
	Oid			optcontext;		/* OID of catalog in which option may appear */
//	bool		is_libpq_opt;	/* true if it's used in libpq */
} PgFdwOption;

static const struct PgFdwOption valid_options[] =
{
	{"endpoint", ForeignTableRelationId},
	{"query_engine", ForeignTableRelationId},
	{"apikey", ForeignTableRelationId},
	{"database", ForeignTableRelationId},
	{"table", ForeignTableRelationId},
	{"query", ForeignTableRelationId},
	{"query_download_dir", ForeignTableRelationId},
	{"import_file_size", ForeignTableRelationId},
	{"atomic_import", ForeignTableRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};

/*
 * Check if the option is valid.
 */
static void
validate_option(DefElem *def, Oid context)
{
	const struct PgFdwOption *opt;
	bool is_valid = false;

	for (opt = valid_options; opt->keyword; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->keyword, def->defname) == 0)
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
		for (opt = valid_options; opt->keyword; opt++)
		{
			if (context == opt->optcontext)
				appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
				                 opt->keyword);
		}

		ereport(ERROR,
		        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
		         errmsg("invalid option \"%s\"", def->defname),
		         buf.len > 0
		         ? errhint("Valid options in this context are: %s", buf.data)
		         : errhint("There are no valid options in this context.")));
	}
}

static int my_strcasecmp(const char *s1, const char *s2)
{
	const unsigned char *p1 = (const unsigned char *) s1;
	const unsigned char *p2 = (const unsigned char *) s2;
	int result;

	if (p1 == p2)
		return 0;

	while ((result = tolower(*p1) - tolower(*p2++)) == 0)
		if (*p1++ == '\0')
			break;

	return result;
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
	char       *query_download_dir = NULL;
	char       *import_file_size = NULL;
	char       *atomic_import = NULL;
	ListCell   *cell;

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
		else if (strcmp(def->defname, "query_download_dir") == 0)
		{
			if (query_download_dir)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));

			query_download_dir = defGetString(def);
		}
		else if (strcmp(def->defname, "import_file_size") == 0)
		{
			const char *nptr = defGetString(def);
			char *end_ptr;
			long size;

			if (import_file_size)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));

			size = strtoul(nptr, &end_ptr, 10);
			if (nptr == end_ptr || size < 0)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("import_file_size must be unsigned integer")));

			import_file_size = (char *) nptr;
		}
		else if (strcmp(def->defname, "atomic_import") == 0)
		{
			if (atomic_import)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));

			atomic_import = defGetString(def);
			if (my_strcasecmp(atomic_import, "true") != 0 &&
			        my_strcasecmp(atomic_import, "false") != 0)
			{
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("'atomic_import' should be boolean")));
			}
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

		if (table != NULL && query != NULL)
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			         errmsg("both table and query can't be specified with treasuredata_fdw foreign tables")));
	}

	PG_RETURN_VOID();
}

void
ExtractFdwOptions(ForeignTable *table, TdFdwOption *fdw_option)
{
	ForeignServer *server;
	ForeignDataWrapper *wrapper;

	List	   *options;
	ListCell   *cell;

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
	fdw_option->query_download_dir = NULL;
	fdw_option->import_file_size = 128 * 1024 * 1024;
	fdw_option->atomic_import = false;

	foreach(cell, options)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "endpoint") == 0)
		{
			fdw_option->endpoint = defGetString(def);
		}
		else if (strcmp(def->defname, "query_engine") == 0)
		{
			fdw_option->query_engine = defGetString(def);
		}
		else if (strcmp(def->defname, "apikey") == 0)
		{
			fdw_option->apikey = defGetString(def);
		}
		else if (strcmp(def->defname, "database") == 0)
		{
			fdw_option->database = defGetString(def);
		}
		else if (strcmp(def->defname, "table") == 0)
		{
			fdw_option->table = defGetString(def);
		}
		else if (strcmp(def->defname, "query") == 0)
		{
			fdw_option->query = defGetString(def);
		}
		else if (strcmp(def->defname, "query_download_dir") == 0)
		{
			fdw_option->query_download_dir = defGetString(def);
		}
		else if (strcmp(def->defname, "import_file_size") == 0)
		{
			const char *nptr = defGetString(def);
			char *end_ptr;
			long size = strtoul(nptr, &end_ptr, 10);

			/* Maybe we don't need to check here since the value is already validated, but just in case... */
			if (nptr != end_ptr && size >= 0)
			{
				fdw_option->import_file_size = size;
			}
		}
		else if (strcmp(def->defname, "atomic_import") == 0)
		{
			if (my_strcasecmp(defGetString(def), "true") == 0)
			{
				fdw_option->atomic_import = true;
			}
			else if (my_strcasecmp(defGetString(def), "false") == 0)
			{
				fdw_option->atomic_import = false;
			}
			else
			{
				elog(ERROR, "treasuredata_fdw: atomic_import should be boolean");
			}
		}
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
	if (fdw_option->table != NULL && fdw_option->query != NULL)
	{
		elog(ERROR, "treasuredata_fdw: both table and query can't be specified with treasuredata_fdw foreign tables");
	}

	elog(DEBUG1, "treasuredata_fdw: endpoint=%s, query_engine=%s, apikey.len=%ld, database=%s, table=%s, query=%s, query_download_dir=%s, import_file_size=%ld, atomic_import=%d",
	     fdw_option->endpoint,
	     fdw_option->query_engine,
	     strlen(fdw_option->apikey),
	     fdw_option->database,
	     fdw_option->table,
	     fdw_option->query,
	     fdw_option->query_download_dir,
	     fdw_option->import_file_size,
	     fdw_option->atomic_import);
}
