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
module Parser ( parse
              ) where

import Text.Printf (printf)

import Scanner (ScannedToken(..), Token(..))

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
  literal    { ScannedToken _ _ ( ValueLiteral $$) }
  relOp      { ScannedToken _ _ ( RelOp $$ ) }
  cmpOp      { ScannedToken _ _ ( CmpOp $$ ) }

  {- as long as the queries dont use these words within the columns names
or within table names, this should work alright -}
  COUNT      { ScannedToken _ _ ( Word "COUNT") }
  table      { ScannedToken _ _ ( Word "table" ) }
  select     { ScannedToken _ _ ( Word "select") }
  project    { ScannedToken _ _ ( Word "project") }
  group      { ScannedToken _ _ ( Word "group") }
  by         { ScannedToken _ _ ( Word "by") }
  top        { ScannedToken _ _ ( Word "top") }
  N          { ScannedToken _ _ ( Word "N"  )  }
  join       { ScannedToken _ _ ( Word "join"  ) }
  antijoin   { ScannedToken _ _ ( Word "antijoin"  ) }
  cross      { ScannedToken _ _ ( Word "crossproduct" ) }
  sys        { ScannedToken _ _ ( Word "sys") }
  as         { ScannedToken _ _ ( Word "as") }
  NOT        { ScannedToken _ _ ( Word "NOT") }
  NULL       { ScannedToken _ _ ( Word "NULL") }
  sum        { ScannedToken _ _ ( Word "sum") }
  sql_add    { ScannedToken _ _ ( Word "sql_add") }
  {- anthing not matched  by the previous special words is dealt with as an identifier -}
  identifier { ScannedToken _ _ ( Word $$  )  }


%% -------------------------------- Grammar -----------------------------------

Program: Tree { $1}

Tree
: Leaf { $1 }
| Node { $1 }

Leaf
: table '(' QualifiedName ')' '[' ExprListNE ']' COUNT
  { Leaf { source=$3, columns = $6 }  }

Node
: RelOp '(' NodeListNE ')' BracketListNE { Node { op = $1, children = $3, arg_lists = $5 } }
| Leaf { $1 }

BracketListNE
: '[' ExprList ']' {  ($2 : []) :: [[(ScalarExpr, Maybe Name)]] }
| '[' ExprList ']' BracketListNE  { $2  : $4 }

RelOp
: select { OpSelect }
| project { OpProject }
| group by { OpGroupBy }
| top N   {  OpTopK }
| join { OpJoin }
| antijoin { OpAntiJoin }
| cross { OpCrossProduct }

NodeListNE
: Node { $1 : [] }
| Node ',' NodeListNE { $1 : $3 }

QualifiedName
: Iden { $1 : [] }
| Iden '.' QualifiedName { $1 : $3 }

Iden {-allowing special words to also be used in any identifier context
we like special words because they allow us to easily express the parsing
of things like 'sys.sql_sum' -}
: sys { "sys" }
| identifier { $1 }

-- NameListNE
-- : NameBind { $1 : [] }
--- | NameBind ',' NameListNE { $1 : $3 }

-- NameBind
-- : QualifiedName { ($1, Nothing)}
--- | QualifiedName as QualifiedName { ($1, Just $3) }

ExprList
: { [] }
| ExprListNE { $1 }

ExprListNE
: ExprBind { [$1] }
| ExprBind ',' ExprListNE { $1 : $3 }

ExprBind
: Expr { ($1, Nothing) }
| Expr as QualifiedName { ($1, Just $3) }

Expr
: QualifiedName { Ref $1 }
| QualifiedName NOT NULL {Ref $1}
| sys '.' sql_add '(' ExprBind ','  ExprBind ')' {- ignoring the aliases here for now -}
  { Binop { opcode = Add, left = fst $5, right = fst $7 } }
  {- todonext : literals and casts. both require typenames  -}


----------------------------------- Haskell -----------------------------------
{

type Name = [String]

data BinaryOp = Gt | Lt | Leq | Geq
  | Eq | Neq {- comp -}
  | Sub | Add | Div | Mul | Mod {- arith -}
  | LogAnd | LogOr
  | BitAnd | BitOr deriving (Eq, Show)

data ScalarExpr =  Literal String
             | Ref Name
             | Binop { opcode :: BinaryOp, left :: ScalarExpr, right :: ScalarExpr }
             | Cast  { typ :: String, arg :: ScalarExpr }  deriving (Eq, Show)

{-
general rel expresssion used for parsing. contains very little checks for sense
-}

data RelOp = OpSelect
           | OpProject
           | OpGroupBy
           | OpTopK
           | OpCrossProduct
           | OpJoin
           | OpSemiJoin
           | OpAntiJoin
           deriving (Eq,Show)

data Rel = Node { op ::RelOp
                , children :: [Rel]
                , arg_lists :: [[(ScalarExpr, Maybe Name)]]  }
           | Leaf { source :: Name, columns :: [(ScalarExpr, Maybe Name)] }
           deriving (Eq, Show)

{- custom types for each expression, used after checking parsed contents -}
-- data RelExpr  = Table { name :: Name, columns :: [(ScalarExpr, Maybe Name)] }
--     | Select { rel :: RelExpr, predicate :: ScalarExpr }
--     | Project { rel :: RelExpr, values :: [(ScalarExpr, Maybe Name)] }
--     | GroupBy { rel :: RelExpr, keys :: [(String, Maybe Name)], values :: [(ScalarExpr, Maybe Name)]  }
--     deriving (Eq,Show)

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
}
