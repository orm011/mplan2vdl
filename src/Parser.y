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
  { Leaf { source=$3, columns=$6 }  }

Node
: identifier '(' NodeListNE ')' BracketListNE { Node { relop = $1, children = $3, arg_lists = $5 } }

TypeSpec
: QualifiedName { TypeSpec { tname = $1, tparams=[]  } }
| QualifiedName '(' NumberListNE ')' { TypeSpec { tname=$1, tparams=$3 } }

NumberListNE
: number { $1 : [] }
| number ',' NumberListNE { $1 : $3 }

BracketListNE
: '[' ExprList ']' {  ($2 : []) :: [[(ScalarExpr, Maybe Name)]] }
| '[' ExprList ']' BracketListNE  { $2  : $4 }


NodeListNE
: Tree { $1 : [] }
| Tree ',' NodeListNE { $1 : $3 }

QualifiedName
: Iden { $1 : [] }
| Iden '.' QualifiedName { $1 : $3 }

Iden
: identifier { $1 }

ExprList
: { [] }
| ExprListNE { $1 }

ExprListNE
: ExprBind { [$1] }
| ExprBind ',' ExprListNE { $1 : $3 }

ExprBind
: Expr  { ($1, Nothing) }
| Expr as QualifiedName { ($1, Just $3) }

Expr
: BasicExprWithAttr  { $1 }
| BasicExprWithAttr infixop Expr
  { Infix  { infixop = $2, left = $1, right = $3 } }
| LikeExpr { $1 }
| InExpr { $1 }

LikeExpr
: ExprBind '!' FILTER like '(' ExprBind ',' BasicExpr ')'
  { Like { arg = $1, negated =  True, pattern = $6, escape = $8 } }
| ExprBind FILTER like '(' ExprBind ',' BasicExpr ')'
  { Like { arg = $1, negated = False, pattern = $5, escape = $7 } }

InExpr
: ExprBind in '(' ExprListNE ')' { In { arg = $1, negated = False, set = $4 } }
| ExprBind notin '(' ExprListNE ')' { In { arg = $1, negated = True, set = $4 } }

BasicExprWithAttr
: BasicExpr AttrList { $1 {-ignore attrlist right now-} }

AttrList
: { [] }
| Attr AttrList { $1 : $2 }

Attr {- TODO: propagate them later. at least ASC should be for ordering
to be correct -}
: NOTNULL { undefined }
| ASC { undefined }
| HASHCOL { undefined }
| JOINIDX QualifiedName { undefined } {- the name refers to a fk constrain in the schema -}
| HASHIDX { undefined } {- sometimes using to refer to fk constraint -}
| FETCH { undefined } {- used in join attributes -}

BasicExpr
: QualifiedName  { Ref $1 }
| QualifiedName '(' ExprList ')'
  { Call { fname = $1, args = $3 } }
| QualifiedName notnil '(' ExprList ')'
  { Call { fname = $1, args = $4 } }
| TypeSpec '[' ExprBind ']' { Cast { tspec=$1, arg=$3 } }
| TypeSpec literal { Literal {tspec=$1, value=$2 } }

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

{- todo: deal with things like x < y < z that show up in the select args-}
data ScalarExpr =  Literal { tspec :: TypeSpec
                           , value::String }
                   | Ref { rname :: Name }
                   | Call { fname :: Name
                          , args :: [(ScalarExpr, Maybe Name)] }
                   | Cast  { tspec :: TypeSpec
                           , arg :: (ScalarExpr, Maybe Name) }
                   | Infix { infixop :: String
                           , left :: ScalarExpr
                           , right :: ScalarExpr }
                   | Like  { arg :: (ScalarExpr, Maybe Name)
                           , negated :: Bool
                           , pattern :: (ScalarExpr, Maybe Name )
                           , escape :: ScalarExpr {- really, should be a literal -}}
                   | In    { arg :: (ScalarExpr, Maybe Name)
                           , negated :: Bool
                           , set :: [(ScalarExpr, Maybe Name)] {- name should probably be NOthing -}
                           }
                   deriving (Eq, Show)

data Rel = Node { relop :: String {- relational op like join -}
                , children :: [Rel]
                , arg_lists :: [[(ScalarExpr, Maybe Name)]]  }
           | Leaf { source :: Name, columns :: [(ScalarExpr, Maybe Name)] }
             {-table scan -}
           deriving (Eq, Show)

parseError :: [ScannedToken] -> Either String a
parseError [] = Left "unexpected EOF"
parseError toks =
  Left $ printf "line %d:%d: unexpected token%s '%s'."
                lineNo
                columnNo
                (if (not $ null $ tail toks) then "s" else "")
                badTokenText
  where firstBadToken = head toks
        lineNo = Scanner.line firstBadToken
        columnNo = Scanner.column firstBadToken
        badTokenText = concatMap (show . extractRawToken) toks

fromString :: String -> Either String Rel
fromString str =
  let (errs, okays) = partitionEithers $ Scanner.scan str
  in
    if errs /= [] then Left $ "Token error: " ++ (show $ head errs)
    else parse okays
}
