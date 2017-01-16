# mplan2vdl
Mplan2vdl translates  monetdb logical plans (mplans) to voodoo vector expressions (aka vdl).

Eg. MonetDB's frontend takes the following SQL:

```sql
plan select
sum(l_extendedprice * l_discount) as revenue
from
lineitem
where
l_shipdate >= date '1994-01-01'
and l_shipdate < date '1994-01-01' + interval '1' year
and l_discount between .06 - 0.01 and .06 + 0.01
and l_quantity < 24;
```
And produces this expression tree in text form:

```
project (
| group by (
| | select (
| | | table(sys.lineitem) [ lineitem.l_quantity,  lineitem.l_extendedprice, 
lineitem.l_discount, lineitem.l_shipdate] COUNT 
| | ) [ 
date "1994-01-01" <= lineitem.l_shipdate < sys.sql_add(date "1994-01-01", month_interval "12"),
sys.sql_sub("6", "1") <= lineitem.l_discount <= sys.sql_add("6", "1"), 
lineitem.l_quantity < decimal(15,2)[tinyint "24"]
]
| ) [  ] [ sys.sum no nil (sys.sql_mul(lineitem.l_extendedprice, lineitem.l_discount)) as L1.L1 ]
) [ L1 as L1.revenue ]
```

Mplan2Vdl parses this textual plan and other metadata files from the database, and uses them to generate a dataflow graph of simple parallel array operations (aka Voodoo).  Its output is a textual representation of the dataflow graph. 
It can optionally apply some simple transforms over either the input our output plans.  Among the transforms we implement are global value numbering and some peephole style optimizations for common special cases.

$ ./tpchrun tests/tpch10noorder/ tests/tpch10noorder/06.sql.mplan --use-cross-product

```
1,Load,lineitem.l_quantity
2,Project,val,Id 1,l_quantity
3,Load,lineitem.l_shipdate
4,Project,val,Id 3,l_shipdate
5,RangeV,val,728294,Id 2,0
6,Greater,val,Id 4,val,Id 5,val
7,Equals,val,Id 5,val,Id 4,val
8,LogicalOr,val,Id 6,val,Id 7,val
9,RangeV,val,728659,Id 2,0
...
40,FoldSum,val,Id 34,val,Id 39,val
41,Project,revenue,Id 40,val
42,MaterializeCompact,Id 41
```

Several common translation and transformation tasks are simple to implement/extend/modify in a language with pattern matching constructs (hence the use of Haskell).

This generated code is meant to be executed by a voodoo implementation, and was used to generate the Voodoo plans in http://www.vldb.org/pvldb/vol9/p1707-pirk.pdf for the tpc-H queries shown in the performance results. NB. this is a research prototype and code quality tradeoffs were made accordingly.

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

## code structure:
The bulk of the functionality is in Vlite.hs and Mplan.hs.
