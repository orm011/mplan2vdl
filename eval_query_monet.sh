#!/bin/bash
set -o pipefail
DB=$1

cat \
| grep -v 'default substitutions' \
| grep -v '^$' \
| grep -v "optimizer_stats()" \
| cat <(echo -n 'plan ') - \
| sed 's/plan//g' \
| mclient $@ $DB -
