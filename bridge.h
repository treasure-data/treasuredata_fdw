#ifndef PG_TREASUREDATA_FDW_BRIDGE_H
#define PG_TREASUREDATA_FDW_BRIDGE_H

extern void* issueQuery(
    const char *apikey, const char *endpoint,
    const char *query_engine, const char *database, const char *query
);

extern int fetchResultRow(
    void *td_query_state, int natts, char **values
);

#endif   /* PG_TREASUREDATA_FDW_BRIDGE_H */

