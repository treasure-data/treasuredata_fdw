/*-------------------------------------------------------------------------
 *
 * test_bridge_import.c
 *        Test program for bridge.c for importing
 *
 * Portions Copyright (c) 2016, Mitsunori Komatsu
 *
 * IDENTIFICATION
 *        test_bridge_import.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bridge.h"

/* For testing */
int main(int argc, char* argv[])
{
	int ret;
	char* apikey = NULL;
	char* database = NULL;
	char* table = NULL;
	int colsize = 0;
	char* coltypes = NULL;
	char* colnames = NULL;
	char* endpoint = NULL;
	char** column_types = NULL;
	char** column_names = NULL;
	void* td_import_state = NULL;

	int i;
	char* token;

	if (argc < 7)
	{
		printf("usage: %s apikey database table colsize coltypes colnames [endpoint]\n", argv[0]);
		return 1;
	}
	apikey = argv[1];
	database = argv[2];
	table = argv[3];
	colsize = atoi(argv[4]);
	coltypes = argv[5];
	colnames = argv[6];

	if (argc >= 8)
	{
		endpoint = argv[7];
	}

	column_types = (char **) malloc(sizeof(char *) * colsize);
	i = 0;
	token = strtok(coltypes, ",");
	while (token != NULL)
	{
		column_types[i] = malloc(64);
		strcpy(column_types[i], token);
		i++;
		token = strtok(NULL, ",");
	}

	column_names = (char **) malloc(sizeof(char *) * colsize);
	i = 0;
	token = strtok(colnames, ",");
	while (token != NULL)
	{
		column_names[i] = malloc(64);
		strcpy(column_names[i], token);
		i++;
		token = strtok(NULL, ",");
	}

	td_import_state = importBegin(
	                      apikey,
	                      endpoint,
	                      database,
	                      table,
	                      colsize,
	                      column_types,
	                      column_names);

	if (td_import_state == NULL)
	{
		printf("Failed to begin import\n");
		return 1;
	}

	return 0;
}

