#!/bin/bash -eu

#declare -r TOP="$(git rev-parse --show-toplevel)"
#cd "$TOP"
set -x
stack build --test --no-run-tests --ghc-options="-O0" --executable-profiling --library-profiling

#test makes sure the test executables get built by default (to ensure they still build)
#noruntest makes sure you build them but not run them (use the test exec ofr that)
#profiling is also used for stack traces

#doesn't work with stack: --alex-options="--ghc --template=\"$TOP/alex\"" --happy-options="-g --info"
