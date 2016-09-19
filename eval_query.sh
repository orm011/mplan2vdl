#!/bin/bash
set -o pipefail

cat \
| grep -v 'default substitutions' \
| grep -v '^$' \
| grep -v "optimizer_stats()" \
| cat <(echo -n 'plan ') - \
| tee /tmp/last_query.sql \
| mclient -f raw tpch01 - \
| tee /tmp/last_query.mplan \
| ./tpch01run --meta --aggserial 2>/tmp/last_query.vdl.err \
| tee /tmp/last_query.vdl \
| sed 's/;;.*//g' \
| curl -H "Content-Type: text/vdl" --data-binary @- http://localhost:25472/voodoo/cpu/dump \
| curl -H "Content-Type: text/voodoo" --data-binary @- http://localhost:8003/constantPropagation/gatherPush/rangePushdown/projectPropagation/projectAfterScatter/zipProjectMerge/removeNoOpGather/removeNoOpGather2/gatherPush/removeFKNoOpGathers/removeMaterializeReturn/removeFoldSelectNoOps/removeNoOpGather/projectPropagation/gatherPush/gatherGatherPush/projectPropagation/rangePushdown/rangePushdown/removeNoOpScatter/removeFKNoOpGathers/removeFoldSelectNoOps/removeNoOpGather/removeNoOpGather2/projectPropagation/fkJoinChain/gatherGatherPush/projectPropagation/removeFKNoOpGathers/removeFoldSelectNoOps/removeNoOpGather/gatherGatherPush/projectPropagation/projectPropagation/zipProjectMerge/rangeUnification/arithmeticSimplification/materializeBeforeReturn/noMultiReturn/materializeToReturn/materializeBeforeReturn\
| curl -v -H "Content-Type: text/voodoo" --data-binary @- http://localhost:25472/voodoo/cpu/run

# curl -v -H "Content-Type: text/voodoo" --data-binary @- http://localhost:8003/materializeToReturn/materializeBeforeReturn \
