#!/bin/bash

PG_DATA_DIR=${PGDATA:=$1}

set -eu

if [[ -z $PG_DATA_DIR ]]; then
    echo 'PGDATA or one parameter that points PostgreSQL data directory is needed'
    exit 1
fi

cat <<EOF
1. Build the FDW and install it
    cd /td_fdw
    make install

2. Start PostgreSQL
    sudo -u postgres /usr/lib/postgresql/9.5/bin/pg_ctl -D /pg start

3. Connect to PostgreSQL
    psql -U (your postgresql user) (your postgresql database)

EOF

docker run -v $(git rev-parse --show-toplevel):/td_fdw -v $PG_DATA_DIR:/pg -it treasuredata_fdw_dev bash
