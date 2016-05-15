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
  infixop    { ScannedToken _ _ ( InfixOp $$ ) }

  {- as long as the queries dont use these words within the columns names
or within table names, this should work alright -}
  COUNT      { ScannedToken _ _ ( Word "COUNT") }
  NOTNULL    { ScannedToken _ _ ( Word "NOT NULL") }
  notnil     { ScannedToken _ _ ( Word "no nil") }
  table     { ScannedToken _ _ ( Word "table") }
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
: Expr { ($1, Nothing) }
| Expr as QualifiedName { ($1, Just $3) }

Expr
: BasicExpr { $1 }
| BasicExpr infixop Expr { Infix  { infixop = $2, left = $1, right = $3 } }

BasicExpr
: QualifiedName { Ref $1 }
| QualifiedName NOTNULL { Ref $1 }
| QualifiedName '(' ExprList ')'
  { Call { fname = $1, args = $3 } }
| QualifiedName notnil '(' ExprList ')'
  { Call { fname = $1, args = $4 } }
| QualifiedName '[' BasicExpr ']' { Cast { tname=$1, arg=$3 } }
| QualifiedName literal { Literal {tname=$1, value=$2  } }

----------------------------------- Haskell -----------------------------------
{

type Name = [String]

{- todo: deal with things like x < y < z that show up in the select args-}
data ScalarExpr =  Literal { tname:: Name,  value::String }
                   | Ref Name
                   | Call { fname :: Name
                          , args :: [(ScalarExpr, Maybe Name)] }
                   | Cast  { tname :: Name
                           , arg :: ScalarExpr }
                   | Infix { infixop :: String
                           , left :: ScalarExpr
                           , right :: ScalarExpr }
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
}
