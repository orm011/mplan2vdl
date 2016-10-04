#!/bin/bash
set -o pipefail

rm -rf /tmp/last_query*

DB="clone01"
META="/home/orm/tpch01metadata"


cat \
| grep -v 'default substitutions' \
| grep -v '^$' \
| grep -v "optimizer_stats()" \
| cat <(echo -n 'plan ') - \
| tee /tmp/last_query.sql \
| mclient -f raw $DB - \
| tee /tmp/last_query.mplan \
| ./tpchrun $META --meta 2>/tmp/last_query.vdl.err \
| tee /tmp/last_query.vdl \
| sed 's/;;.*//g' \
| curl -H "Content-Type: text/vdl" --data-binary @- http://localhost:25472/voodoo/cpu/dump \
| tee /tmp/last_query.voodoo \
| curl -H "Content-Type: text/voodoo" --data-binary @- http://localhost:8003/constantPropagation/gatherPush/rangePushdown/projectPropagation/projectAfterScatter/zipProjectMerge/removeNoOpGather/removeNoOpGather2/gatherPush/removeFKNoOpGathers/removeMaterializeReturn/removeFoldSelectNoOps/removeNoOpGather/projectPropagation/gatherPush/gatherGatherPush/projectPropagation/rangePushdown/rangePushdown/removeNoOpScatter/removeFKNoOpGathers/removeFoldSelectNoOps/removeNoOpGather/removeNoOpGather2/projectPropagation/fkJoinChain/gatherGatherPush/projectPropagation/removeFKNoOpGathers/removeFoldSelectNoOps/removeNoOpGather/gatherGatherPush/projectPropagation/projectPropagation/zipProjectMerge/rangeUnification/arithmeticSimplification/materializeBeforeReturn/noMultiReturn/materializeToReturn/materializeBeforeReturn\
| curl -v -H "Content-Type: text/voodoo" --data-binary @- http://localhost:25472/voodoo/cpu/run \
| tee /tmp/last_query.json\
| ./resolve.py $META/dictionary.csv

# curl -v -H "Content-Type: text/voodoo" --data-binary @- http://localhost:8003/materializeToReturn/materializeBeforeReturn \
