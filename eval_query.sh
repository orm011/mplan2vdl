#!/bin/bash
set -o pipefail

# | grep -v 'default substitutions' \
# | grep -v '^$' \
# | grep -v "optimizer_stats()" \
cat \
| cat <(echo -n 'plan ') - \
| mclient -f raw tpch10 -          
# | ./tpch10run --aggserial 2>/dev/null \
# | cat

# | curl -H "Content-Type: text/vdl" --data-binary @- http://arachnophobia.csail.mit.edu:25472/voodoo/cpu/dump \
# | curl -v -H "Content-Type: text/voodoo" --data-binary @- http://arachnophobia.csail.mit.edu:9003/materializeToReturn/materializeBeforeReturn \
# | curl -v -H "Content-Type: text/voodoo" --data-binary @- http://arachnophobia.csail.mit.edu:25472/voodoo/cpu/run
