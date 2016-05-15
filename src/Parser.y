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
  literal    { ScannedToken _ _ (ValueLiteral $$) }
  ','        { ScannedToken _ _ Comma }
  '.'        { ScannedToken _ _ Dot }
  relOp     { ScannedToken _ _ (RelOp $$ ) }
  cmpOp     { ScannedToken _ _ (CmpOp $$ ) }
  table     { ScannedToken _ _ ( Keyword "table" ) }
  select    { ScannedToken _ _ ( Keyword "select") }
  project    { ScannedToken _ _ ( Keyword "project") }
  COUNT     { ScannedToken _ _  (Keyword "COUNT") }
  sys { ScannedToken _ _  (Identifier "sys") }
  identifier      {  ScannedToken _ _ ( Identifier $$  )  }
  as        {  ScannedToken _ _ ( Keyword "as") }
  NOT       {  ScannedToken _ _ ( Keyword "NOT") }
  NULL       {  ScannedToken _ _ ( Keyword "NULL") }
  sum       {  ScannedToken _ _ ( Keyword "sum") }
  sql_add   {  ScannedToken _ _ ( Keyword "sql_add") }

%% -------------------------------- Grammar -----------------------------------

Program: RelExpr { $1}

RelExpr
: Table { $1 }
| Project { $1 }

{- extra check: this expression list should be non-emtpy and made out of columns only -}
Table
: table '(' QualifiedName ')' '[' ExprList ']' COUNT
  { Table { name=$3, columns = $6 }  }

Project
: project '(' RelExpr ')' '[' ExprList ']'
  {  Project { rel = $3,  values = $6 } }

QualifiedName
: Iden { [$1] }
| Iden '.' QualifiedName { $1 : $3 }

Iden {-annoying sys.* -}
: sys { "sys" }
| identifier { $1 }


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
  { Binop { op = Add, left = fst $5, right = fst $7 } }
  {- todonext : literals and casts. both require typenames  -}


----------------------------------- Haskell -----------------------------------
{

type Name = [String]

data BinaryOp = Gt | Lt | Leq | Geq
  | Eq | Neq {- comp -}
  | Sub | Add | Div | Mul | Mod {- arith -}
  | And | Or deriving (Eq, Show)

data ScalarExpr =  Literal String
             | Ref Name
             | Binop { op :: BinaryOp, left :: ScalarExpr, right :: ScalarExpr }
             | Cast  { typ :: String, arg :: ScalarExpr }  deriving (Eq, Show)

data RelExpr  = Table { name :: Name, columns :: [(ScalarExpr, Maybe Name)] }
    | Select { rel :: RelExpr, predicate :: ScalarExpr }
    | Project { rel :: RelExpr, values :: [(ScalarExpr, Maybe Name)] }
    | GroupBy { rel :: RelExpr, keys :: [(String, Maybe Name)], values :: [(ScalarExpr, Maybe Name)]  }
    deriving (Eq,Show)

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
