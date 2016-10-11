# mplan2vdl
Mplan2vdl translates  monetdb logical plans (mplans) to voodoo vector expressions (aka vdl).
The logical plans are what you get from prepending the word 'plan' to a sql query in MonetDb.

This generated code is meant to be read by a voodoo implementation, and was used to 
generate the Voodoo plans in http://www.vldb.org/pvldb/vol9/p1707-pirk.pdf for most tpc-H queries.

## Building it:
1) install the Stack package manager for your system (this is used to manage haskell dependencies)
https://docs.haskellstack.org/en/stable/install_and_upgrade/

2) you can now build it with ./build.sh

## Running it:
The main executable is ./mplan2vdl. You can use --help to see the options.
In addition to the input plan, ./mplan2vdl needs multiple metadata files obtainable via MonetDB.
There are several wrapper scripts to help use the translator that pass the necessary paths for 
a sample tpch01 database.

