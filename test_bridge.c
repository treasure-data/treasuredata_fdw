#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "bridge.h"

/* For testing */
int main(int argc, char* argv[])
{
	int ret;
	char* apikey = NULL;
	char* database = NULL;
	int num_of_select_col;
	char* query = NULL;
	char* endpoint = NULL;
	void* td_query_state = NULL;

	char** values;

	if (argc < 5)
	{
		printf("usage: %s apikey database num_of_select_col query [endpoint]\n", argv[0]);
		return 1;
	}
	apikey = argv[1];
	database = argv[2];
	num_of_select_col = atoi(argv[3]);
	query = argv[4];

	if (argc >= 6)
	{
		endpoint = argv[5];
	}

	td_query_state = issueQuery(apikey, endpoint, "presto", database, query);
	if (td_query_state == NULL)
	{
		printf("Failed to execute query\n");
		return 1;
	}

	while (1)
	{
		int i;

		values = (char **) malloc(sizeof(char *) * num_of_select_col);
		ret = fetchResultRow(td_query_state, num_of_select_col, values);

		printf("============================== ret: %d ==================================\n", ret);

		if (ret != 0)
			break;

		for (i = 0; i < num_of_select_col; i++)
		{
			printf("i=%d, value=%s\n", i, values[i]);
			free(values[i]);
		}
	}

	return 0;
}

