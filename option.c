/*-------------------------------------------------------------------------
 *
 * option.c
 *		  FDW option handling for postgres_fdw
 *
 * Portions Copyright (c) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/postgres_fdw/option.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "treasuredata_fdw.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "utils/builtins.h"


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

	/* Sentinel */
	{NULL, InvalidOid}
};

/*
 * Check if the option is valid.
 */
static bool
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
	 * Check that only options supported by treasuredata_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		/*
		 * Check if the option is valid with looking up Option.
		 */
        // FIXME: Check the result....
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

void
ExtractFdwOptions(ForeignTable *table, TdFdwOption *fdw_option)
{
	ForeignServer *server;
	ForeignDataWrapper *wrapper;

	List	   *options;
	ListCell   *cell, *prev;

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
	if (fdw_option->table == NULL)
	{
		elog(ERROR, "treasuredata_fdw: table is required for treasuredata_fdw foreign tables");
	}

	elog(DEBUG1, "treasuredata_fdw: endpoint=%s, query_engine=%s, apikey=%s, database=%s, table=%s",
	     fdw_option->endpoint,
	     fdw_option->query_engine,
	     fdw_option->apikey,
	     fdw_option->database,
	     fdw_option->table);
}
#if 0
/*
 * Valid options for postgres_fdw.
 * Allocated and filled in InitPgFdwOptions.
 */
static PgFdwOption *postgres_fdw_options;

/*
 * Valid options for libpq.
 * Allocated and filled in InitPgFdwOptions.
 */
static PQconninfoOption *libpq_options;

/*
 * Helper functions
 */
static void InitPgFdwOptions(void);
static bool is_valid_option(const char *keyword, Oid context);
static bool is_libpq_option(const char *keyword);
#endif

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses postgres_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
PG_FUNCTION_INFO_V1(postgres_fdw_validator);

#if 0
Datum
postgres_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;
    char       *endpoint = NULL;
    char       *query_engine = NULL;
    char       *apikey = NULL;
    char       *database = NULL;
    char       *table = NULL;

    /* Build our options lists if we didn't yet. */
	InitPgFdwOptions();

	/*
	 * Check that only options supported by postgres_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!is_valid_option(def->defname, catalog))
		{
			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			PgFdwOption *opt;
			StringInfoData buf;

			initStringInfo(&buf);
			for (opt = postgres_fdw_options; opt->keyword; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->keyword);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s",
							 buf.data)));
		}

		/*
		 * Validate option value, when we can do so without any context.
		 */
		if (strcmp(def->defname, "use_remote_estimate") == 0 ||
			strcmp(def->defname, "updatable") == 0)
		{
			/* these accept only boolean values */
			(void) defGetBoolean(def);
		}
		else if (strcmp(def->defname, "fdw_startup_cost") == 0 ||
				 strcmp(def->defname, "fdw_tuple_cost") == 0)
		{
			/* these must have a non-negative numeric value */
			double		val;
			char	   *endp;

			val = strtod(defGetString(def), &endp);
			if (*endp || val < 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("%s requires a non-negative numeric value",
								def->defname)));
		}
		else if (strcmp(def->defname, "extensions") == 0)
		{
			/* check list syntax, warn about uninstalled extensions */
			(void) ExtractExtensionList(defGetString(def), true);
		}
		else if (strcmp(def->defname, "fetch_size") == 0)
		{
			int			fetch_size;

			fetch_size = strtol(defGetString(def), NULL, 10);
			if (fetch_size <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("%s requires a non-negative integer value",
								def->defname)));
		}
        else if (strcmp(def->defname, "endpoint") == 0)
		{
			if (endpoint)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));

			endpoint = defGetString(def);
		}
		else if (strcmp(def->defname, "query_engine") == 0)
		{
			query_engine = defGetString(def);
		}
		else if (strcmp(def->defname, "apikey") == 0)
		{
			apikey = defGetString(def);
		}
		else if (strcmp(def->defname, "database") == 0)
		{
			database = defGetString(def);
		}
		else if (strcmp(def->defname, "table") == 0)
		{
			table = defGetString(def);
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

		if (table == NULL)
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			         errmsg("table or query is required for treasuredata_fdw foreign tables")));
	}

	PG_RETURN_VOID();
}

/*
 * Initialize option lists.
 */
static void
InitPgFdwOptions(void)
{
	int			num_libpq_opts;
	PQconninfoOption *lopt;
	PgFdwOption *popt;

	/* non-libpq FDW-specific FDW options */
	static const PgFdwOption non_libpq_options[] = {
		{"schema_name", ForeignTableRelationId, false},
		{"table_name", ForeignTableRelationId, false},
		{"column_name", AttributeRelationId, false},
		/* use_remote_estimate is available on both server and table */
		{"use_remote_estimate", ForeignServerRelationId, false},
		{"use_remote_estimate", ForeignTableRelationId, false},
		/* cost factors */
		{"fdw_startup_cost", ForeignServerRelationId, false},
		{"fdw_tuple_cost", ForeignServerRelationId, false},
		/* shippable extensions */
		{"extensions", ForeignServerRelationId, false},
		/* updatable is available on both server and table */
		{"updatable", ForeignServerRelationId, false},
		{"updatable", ForeignTableRelationId, false},
		/* fetch_size is available on both server and table */
		{"fetch_size", ForeignServerRelationId, false},
		{"fetch_size", ForeignTableRelationId, false},

		{"endpoint", ForeignTableRelationId, false},
		{"query_engine", ForeignTableRelationId, false},
		{"apikey", ForeignTableRelationId, false},
		{"database", ForeignTableRelationId, false},
		{"table", ForeignTableRelationId, false},

		{NULL, InvalidOid, false}
	};

	/* Prevent redundant initialization. */
	if (postgres_fdw_options)
		return;

	/*
	 * Get list of valid libpq options.
	 *
	 * To avoid unnecessary work, we get the list once and use it throughout
	 * the lifetime of this backend process.  We don't need to care about
	 * memory context issues, because PQconndefaults allocates with malloc.
	 */
	libpq_options = PQconndefaults();
	if (!libpq_options)			/* assume reason for failure is OOM */
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("out of memory"),
			 errdetail("could not get libpq's default connection options")));

	/* Count how many libpq options are available. */
	num_libpq_opts = 0;
	for (lopt = libpq_options; lopt->keyword; lopt++)
		num_libpq_opts++;

	/*
	 * Construct an array which consists of all valid options for
	 * postgres_fdw, by appending FDW-specific options to libpq options.
	 *
	 * We use plain malloc here to allocate postgres_fdw_options because it
	 * lives as long as the backend process does.  Besides, keeping
	 * libpq_options in memory allows us to avoid copying every keyword
	 * string.
	 */
	postgres_fdw_options = (PgFdwOption *)
		malloc(sizeof(PgFdwOption) * num_libpq_opts +
			   sizeof(non_libpq_options));
	if (postgres_fdw_options == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	popt = postgres_fdw_options;
	for (lopt = libpq_options; lopt->keyword; lopt++)
	{
		/* Hide debug options, as well as settings we override internally. */
		if (strchr(lopt->dispchar, 'D') ||
			strcmp(lopt->keyword, "fallback_application_name") == 0 ||
			strcmp(lopt->keyword, "client_encoding") == 0)
			continue;

		/* We don't have to copy keyword string, as described above. */
		popt->keyword = lopt->keyword;

		/*
		 * "user" and any secret options are allowed only on user mappings.
		 * Everything else is a server option.
		 */
		if (strcmp(lopt->keyword, "user") == 0 || strchr(lopt->dispchar, '*'))
			popt->optcontext = UserMappingRelationId;
		else
			popt->optcontext = ForeignServerRelationId;
		popt->is_libpq_opt = true;

		popt++;
	}

	/* Append FDW-specific options and dummy terminator. */
	memcpy(popt, non_libpq_options, sizeof(non_libpq_options));
}

/*
 * Check whether the given option is one of the valid postgres_fdw options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option(const char *keyword, Oid context)
{
	PgFdwOption *opt;

	Assert(postgres_fdw_options);		/* must be initialized already */

	for (opt = postgres_fdw_options; opt->keyword; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->keyword, keyword) == 0)
			return true;
	}

	return false;
}

/*
 * Check whether the given option is one of the valid libpq options.
 */
static bool
is_libpq_option(const char *keyword)
{
	PgFdwOption *opt;

	Assert(postgres_fdw_options);		/* must be initialized already */

	for (opt = postgres_fdw_options; opt->keyword; opt++)
	{
		if (opt->is_libpq_opt && strcmp(opt->keyword, keyword) == 0)
			return true;
	}

	return false;
}

/*
 * Generate key-value arrays which include only libpq options from the
 * given list (which can contain any kind of options).  Caller must have
 * allocated large-enough arrays.  Returns number of options found.
 */
int
ExtractConnectionOptions(List *defelems, const char **keywords,
						 const char **values)
{
	ListCell   *lc;
	int			i;

	/* Build our options lists if we didn't yet. */
	InitPgFdwOptions();

	i = 0;
	foreach(lc, defelems)
	{
		DefElem    *d = (DefElem *) lfirst(lc);

		if (is_libpq_option(d->defname))
		{
			keywords[i] = d->defname;
			values[i] = defGetString(d);
			i++;
		}
	}
	return i;
}

/*
 * Parse a comma-separated string and return a List of the OIDs of the
 * extensions named in the string.  If any names in the list cannot be
 * found, report a warning if warnOnMissing is true, else just silently
 * ignore them.
 */
List *
ExtractExtensionList(const char *extensionsString, bool warnOnMissing)
{
	List	   *extensionOids = NIL;
	List	   *extlist;
	ListCell   *lc;

	/* SplitIdentifierString scribbles on its input, so pstrdup first */
	if (!SplitIdentifierString(pstrdup(extensionsString), ',', &extlist))
	{
		/* syntax error in name list */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter \"%s\" must be a list of extension names",
						"extensions")));
	}

	foreach(lc, extlist)
	{
		const char *extension_name = (const char *) lfirst(lc);
		Oid			extension_oid = get_extension_oid(extension_name, true);

		if (OidIsValid(extension_oid))
		{
			extensionOids = lappend_oid(extensionOids, extension_oid);
		}
		else if (warnOnMissing)
		{
			ereport(WARNING,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("extension \"%s\" is not installed",
							extension_name)));
		}
	}

	list_free(extlist);
	return extensionOids;
}
#endif
