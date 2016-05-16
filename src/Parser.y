-- Parser -- Decaf parser                                       -*- haskell -*-
-- Copyright (C) 2013  Benjamin Barenblat <bbaren@mit.edu>
--
-- This file is a part of decafc.
--
-- decafc is free software: you can redistribute it and/or modify it under the
-- terms of the MIT (X11) License as described in the LICENSE file.
--
-- decafc is distributed in the hope that it will be useful, but WITHOUT ANY
-- WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
-- FOR A PARTICULAR PURPOSE.  See the X11 license for more details.
{
module Parser ( parse,
                fromString
              ) where

import Text.Printf (printf)
import Data.Either(isRight,partitionEithers)
import Scanner (ScannedToken(..), Token(..), scan)

}


--------------------------------- Directives ----------------------------------

%name parse
%error { parseError }
%monad { Either String }

%tokentype { ScannedToken }

%token
  '['        { ScannedToken _ _ LBrack }
  ']'        { ScannedToken _ _ RBrack }
  '('        { ScannedToken _ _ LParen }
  ')'        { ScannedToken _ _ RParen }
  ','        { ScannedToken _ _ Comma }
  '.'        { ScannedToken _ _ Dot }
  '!'        { ScannedToken _ _ ( Oper "!" ) }
  literal    { ScannedToken _ _ ( ValueLiteral $$) }
  infixop    { ScannedToken _ _ ( Oper $$ ) }
  number     { ScannedToken _ _ ( NumberLiteral $$) }

  {- as long as the queries dont use these words within the columns names
or within table names, this should work alright -}
  COUNT      { ScannedToken _ _ ( Word "COUNT") }
  NOTNULL    { ScannedToken _ _ ( Word "NOT NULL") }
  HASHCOL    { ScannedToken _ _ ( Word "HASHCOL") }
  JOINIDX    { ScannedToken _ _ ( Word "JOINIDX") }
  HASHIDX    { ScannedToken _ _ ( Word "HASHIDX") }
  FETCH      { ScannedToken _ _ ( Word "FETCH")  }
  ASC        { ScannedToken _ _ ( Word "ASC") }
  FILTER     { ScannedToken _ _ ( Word "FILTER") }
  like       { ScannedToken _ _ ( Word "like") }
  in         { ScannedToken _ _ ( Word "in") }
  notin      { ScannedToken _ _ ( Word "notin") }
  notnil     { ScannedToken _ _ ( Word "no nil") }
  table      { ScannedToken _ _ ( Word "table") }
  as         { ScannedToken _ _ ( Word "as") }
  {- anthing not matched  by the previous special words is dealt with as an identifier -}
  identifier { ScannedToken _ _ ( Word $$  )  }


%% -------------------------------- Grammar -----------------------------------

Program: Tree { $1}

Tree
: Leaf { $1 }
| Node { $1 }

Leaf
: table '(' QualifiedName ')' '[' ExprListNE ']' COUNT
{- within the braces, we regard comma separated lists of expressions as an infix comma operator -}
  { Leaf { source=$3, columns=$6 }  }

Node
: identifier '(' NodeListNE ')' BracketListNE { Node { relop = $1, children = $3, arg_lists = $5 } }

TypeSpec
: QualifiedName { TypeSpec { tname = $1, tparams=[]  } }
| QualifiedName '(' NumberListNE ')' { TypeSpec { tname=$1, tparams=$3 } }

NumberListNE
: number { ($1 : []) :: [Int] }
| number ',' NumberListNE { ($1 : $3) :: [Int] }

BracketListNE
: '[' ExprList ']' {  ($2 : []) :: [[Expr]] }
| '[' ExprList ']' BracketListNE  { ($2  : $4) :: [[Expr]] }

NodeListNE
: Tree { ( $1 : [] ) :: [Rel] }
| Tree ',' NodeListNE { ($1 : $3) :: [Rel] }

QualifiedName
: Iden { ($1 : []) :: [String] }
| Iden '.' QualifiedName { ($1 : $3) :: [String] }

Iden
: identifier { $1  :: String }

{- we use this list only at the top level of nesting, for nested commas used as 'and' use ExprComma.
Note that this list allows things like [foo as bar] and also empty list [] -}
ExprList
: { [] }
| ExprListNE { $1 }

ExprListNE
: Expr { [$1] }
| Expr ',' ExprListNE { $1 : $3 }


{- comma is the weakest in the hierarchy. other operators take precedence for now (at least OR does,
in reality)
  The following makes comma be right-associative, which I assume is okay.. This expression only shows up nested ( a or (b and c))
this only allows things like [a, b] but not [a as b]
-}
-- ExprComma
-- : Expr { ($1 : [] ):: [ScalarExpr] }
--- | Expr ',' ExprComma { ($1 : $3) :: [ScalarExpr] }

Expr {- top level definition  -}
: ExprBind { $1 :: Expr }

ExprBind {- allows for the aliasing that happens sometimes -}
: ExprNoComma  { Expr { expr=($1 :: ScalarExpr ), alias=Nothing } }
| ExprNoComma  as QualifiedName { Expr { expr=($1 :: ScalarExpr ), alias= Just $3 } }

{- note to self on operator precedences:
For the most part, parenthesis are explicit in the plans, except for commas:

In the monet plan,  AND (in the where clause) becomes ',' and
FILTER, IN and OR expressions are not parenthesized within it. (so they must bind more strongly than ',')

If OR is forced to  be  main operator at the sql level, then the AND expressions
get parenthesized and OR appears at the top level.

Here are some examples:

-- And (aka ',') is parenthesised in the plan on the RHS of the OR.
sql>plan select r_name from region where region.r_regionkey  < 40 or region.r_regionkey < 100 and r_name like '%oston' and r_name in (1,2);
project (
| select (
"| | table(sys.region) [ region.r_regionkey NOT NULL HASHCOL , region.r_name NOT NULL ] COUNT "
"| ) [ (region.r_regionkey NOT NULL HASHCOL  < int[tinyint ""40""]) or (region.r_regionkey NOT NULL HASHCOL  < int[tinyint ""100""], char[region.r_name NOT NULL] FILTER like (char[char(6) ""%oston""], char """"), tinyint[region.r_name NOT NULL] as region.r_name in (tinyint ""1"", tinyint ""2"")) ]"
) [ region.r_name NOT NULL ]


-- notice how OR is not parenthesised in the plan.
sql>plan select r_name from region where (region.r_regionkey  < 40 or region.r_regionkey < 100) and r_name like '%oston' and r_name in (1,2);

project (
| select (
"| | table(sys.region) [ region.r_regionkey NOT NULL HASHCOL , region.r_name NOT NULL ] COUNT "
"| ) [ (region.r_regionkey NOT NULL HASHCOL  < int[tinyint ""40""]) or (region.r_regionkey NOT NULL HASHCOL  < int[tinyint ""100""]), char[region.r_name NOT NULL] FILTER like (char[char(6) ""%oston""], char """"), tinyint[region.r_name NOT NULL] as region.r_name in (tinyint ""1"", tinyint ""2"") ]"
) [ region.r_name NOT NULL ]

other than comma, comparison operators sometimes come in non-parenthesised trees as well:

sql>plan select (l_quantity < 100) as foo from lineitem where l_orderkey < l_partkey and l_partkey < l_suppkey;

project (
| select (
"| | table(sys.lineitem) [ lineitem.l_orderkey NOT NULL HASHCOL , lineitem.l_partkey NOT NULL, lineitem.l_suppkey NOT NULL, lineitem.l_quantity NOT NULL ] COUNT "
| ) [ lineitem.l_orderkey NOT NULL HASHCOL  < lineitem.l_partkey NOT NULL < lineitem.l_suppkey NOT NULL ]
") [ sys.<(lineitem.l_quantity NOT NULL, decimal(15,2)[tinyint ""100""]) as L.foo ]"

-}


{- things like <, which show up as trees, or OR which do not fit within the BasicExpr list.
note:
Right now we allow them to have the same associativity (ie 1 < 2 or 3 -> 1 < (2 or 3),
but in practice OR always shows up with parens around its two arguments.
-}
ExprNoComma
: BasicExpr  { $1 :: ScalarExpr }
| BasicExpr infixop ExprNoComma
  { Infix  { infixop = $2, left = ($1 :: ScalarExpr), right = ($3 :: ScalarExpr) } }

{-attributes only seem to show up next to column refernces, and sometimes functions -}
BasicExpr
: BasicExprBare AttrList { $1 :: ScalarExpr }

AttrList
: { [] }
| Attr AttrList { $1 : $2 }

Attr
{- NOTNULL shows up after columns marked NOT NULL in the schena, and sometimes function calls like sys.sum() -}
: NOTNULL { undefined }
{- ASC shows up in 2nd bracket list of projects, after column references when ORDER BY is in the query
Desc does not produce an explicit annotation.-}
| ASC { undefined }
{- HASHCOL shows up next to column references, those that are primary keys -}
| HASHCOL { undefined }
{- JOINIDX the name refers to a fk constraint name (which looks like a column name) in the schema on the right -}
| JOINIDX QualifiedName { undefined }
{- HASHIDX shows up on table() operators. foreign key constraint name on the left-}
| HASHIDX { undefined }
{- shows up in some join bracket lists -}
| FETCH { undefined }

{- expression with no attributes yet.
  Almost always used followed by attributes, so use BasicExpr instead of this.
-}
BasicExprBare
: QualifiedName  { Ref $1 }
| QualifiedName '(' ExprList ')'
  { Call { fname = $1, args = $3 } }
| QualifiedName notnil '(' ExprList ')'
  { Call { fname = $1, args = $4 } }
| TypeSpec '[' Expr ']' { Cast { tspec=$1, value=$3 } }
| TypeSpec literal { Literal {tspec=$1, stringRep=$2 } }
| LikeExpr { $1 }
| InExpr { $1 }
| '(' ExprListNE ')' { Nested $2 }

{- for now, only seen like after a char[] cast, which is a basic expression-}
LikeExpr
:  BasicExpr FILTER like '(' Expr ',' BasicExprBare ')' {- it should be a 2 elt list, last one being a literal -}
  { Like { arg = $1, negated = False, pattern=$5, escape=$7 } }
|  BasicExpr '!' FILTER like '(' Expr ',' BasicExprBare ')' {- it should be a 2 elt list, last one being a literal -}
  { Like { arg = $1, negated =  True, pattern=$6, escape=$8 } }

{- for now, only seen IN after a column ref (with NOT NULL in it), and after
some function call. the internal expr is a comma infix -}
InExpr
: BasicExpr  in '(' ExprList ')' { In { arg = $1, negated = False, set = $4 } }
| BasicExpr notin '(' ExprList ')' { In { arg = $1, negated = True, set = $4 } }

----------------------------------- Haskell -----------------------------------
{

type Name = [String]

{- eg decimal(15,2) , or smallint  -}
data TypeSpec = TypeSpec { tname :: Name
                         , tparams :: [Int] } deriving (Eq,Show)

{- for now, parse but drop the column attributes.
maybe will take a look again later.
do they beling only with references (like L1.L1 Asc), or also
with expressions (like NOT NULL seems to be)

-- data Order  =  OrderAsc | OrderDefault  deriving (Eq,Show)
-- data HashCol = HashCol | PlainCol deriving (Eq,Show)
-- data Nullable = NotNull | Nullable deriving (Eq,Show)
                         -- , rorder=Order
                         -- , rhash=Hashcol
                         -- , rnullable=Nullable
-}

data Expr = Expr { expr :: ScalarExpr, alias :: Maybe Name } deriving (Eq,Show)

{- todo: deal with things like x < y < z that show up in the select args-}
data ScalarExpr =  Literal { tspec :: TypeSpec
                           , stringRep :: String
                           }
                   | Ref   { rname :: Name }
                   | Call  { fname :: Name
                           , args :: [Expr]
                           }
                   | Cast  { tspec :: TypeSpec
                           , value :: Expr
                           }
                   | Infix { infixop :: String
                           , left :: ScalarExpr
                           , right :: ScalarExpr
                           }
                   | Like  { arg :: ScalarExpr
                           , negated :: Bool
                           , pattern :: Expr
                           , escape :: ScalarExpr
                           }
                   | In    { arg :: ScalarExpr
                           , negated :: Bool
                           , set :: [Expr]  {- alias should probably be Nothing -}
                           }
                   | Nested [Expr] {-its here just allow types to check without modifying the grammar -}

                   deriving (Eq, Show)

data Rel = Node { relop :: String {- relational op like join -}
                , children :: [Rel]
                , arg_lists :: [[Expr]]  }
           | Leaf { source :: Name, columns :: [Expr] }
             {-table scan -}
           deriving (Eq, Show)

parseError :: [ScannedToken] -> Either String a
parseError [] = Left "unexpected EOF"
parseError toks =
  Left $ printf "At %d:%d. unexpected token%s (at most %d shown): '%s'."
                lineNo
                columnNo
                (if (not $ null $ tail toks) then "s" else "")
                numToks
                badTokenText
  where numToks = 10
        firstBadToken = head toks
        lineNo = Scanner.line firstBadToken
        columnNo = Scanner.column firstBadToken
        badTokenText = concatMap ((++ "  ") . show . extractRawToken) (take numToks toks)

fromString :: String -> Either String Rel
fromString str =
  let (errs, okays) = partitionEithers $ Scanner.scan str
  in
    if errs /= [] then Left $ "Token error: " ++ (show $ head errs)
    else parse okays
}
