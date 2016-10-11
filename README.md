# mplan2vdl
Mplan2vdl translates  monetdb logical plans (mplans) to voodoo vector expressions (aka vdl).
The logical plans are what you get from prepending the word 'plan' to a sql query in MonetDb.

This generated code is meant to be read by a voodoo implementation, and was used to 
generate the Voodoo plans in http://www.vldb.org/pvldb/vol9/p1707-pirk.pdf for most tpc-H queries.

## Building it the first time:
* install the Stack package manager for your system (this is used to manage haskell dependencies)
by following the instructions @ https://docs.haskellstack.org/en/stable/install_and_upgrade/#installupgrade
* run 'stack setup'
* you can now use ./build.sh 

## Running it:
The main executable is linked to through ./mplan2vdl. You can use --help to see the options.

In addition to the input plan ./mplan2vdl needs multiple (4) metadata files obtainable via MonetDB.
These are:
* A schema of the tables (obtained msqldump -D) used to get the data types, column names, primary and foreign key relations.
* Storage information (obtained though "select * from storage;") to obtain internal column names and storage types (type widths)
* Bound information (obtained through the Resolver code with the --bounds flag) to get data bounds and counts for each column in the database.
* Dictionary (obtained through the Resolver code) to translate string literals in the query into their internal representation.


There are several wrapper scripts to help use the translator that pass the necessary paths for 
a sample tpch01 database. (TODO add these to the repo)

