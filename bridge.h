/*-------------------------------------------------------------------------
 *
 * bridge.h
 *        Bridge between C and Rust
 *
 * Portions Copyright (c) 2016, Mitsunori Komatsu
 *
 * IDENTIFICATION
 *        bridge.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TREASUREDATA_FDW_BRIDGE_H
#define TREASUREDATA_FDW_BRIDGE_H

extern void* issueQuery(
    const char *apikey, const char *endpoint,
    const char *query_engine, const char *database, const char *query
);

extern int fetchResultRow(
    void *td_query_state, int natts, char **values
);

#endif   /* TREASUREDATA_FDW_BRIDGE_H */

