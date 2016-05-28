-- -*- haskell -*-
{
module Parser ( parse
              , fromString
              , Rel(..)
              , ScalarExpr(..)
              , Expr(..)
              , TypeSpec(..)
              , Attr(..)
              ) where

import Text.Printf (printf)
import Data.Either(isRight,partitionEithers)
import Scanner (ScannedToken(..), Token(..), scan)
import Control.DeepSeq(NFData)
import GHC.Generics (Generic)
import Text.Groom
import Debug.Trace

import Name(Name(..))
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
  '!'        { ScannedToken _ _ ( Word "!" ) }
  literal    { ScannedToken _ _ ( ValueLiteral $$) }
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
: identifier   { TypeSpec { tname = $1, tparams=[]  } }
| identifier '(' NumberListNE ')' { TypeSpec { tname=$1, tparams=$3 } }

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
: QualifiedNameB { Name $1 }

QualifiedNameB
: identifier { ( [$1] ) }
| identifier '.' QualifiedNameB { $1 : $3 }

ExprList
: { [] }
| ExprListNE { $1 }

ExprListNE
: Expr { [$1] }
| Expr ',' ExprListNE { $1 : $3 }

Expr {- top level definition  -}
: ExprNoComma { $1 :: Expr }

{-
Note to self on operator precedences:
Parenthesis are explicit in most plans, except for commas and relation operations (<=, <).

in where conditions AND (in the where clause) becomes ',' and
FILTER, IN and OR expressions are not parenthesized within it.
(so they must bind more strongly than ',')

If OR is forced to  be  main operator at the sql level, then the AND expressions
get parenthesized and OR appears at the top level.

See examples in detailed_tests.txt
-}

{-
ExprNoComma: expressions that have no comma within them bind tighter than those with comma.
things like <, which show up as trees. or OR which do not fit within the BasicExpr list.

Right now we allow them to have the same associativity (ie 1 < 2 or 3 -> 1 < (2 or 3)),
but in practice OR always shows up with parens around its two arguments.
-}
ExprNoComma
: ExprBind  { $1 :: Expr }
| ExprBind identifier ExprBind   { Expr { expr=Infix  { infixop = $2, left = ($1 :: Expr), right = ($3 :: Expr) }, alias=Nothing } }
| ExprBind  identifier ExprBind  identifier ExprBind {
    Expr { expr=Interval { ifirst=$1
                         , firstop=$2
                         , imiddle=$3
                         , secondop=$4
                         , ilast=$5
                         }
         , alias=Nothing
         }
    }

ExprBind {- allows for the aliasing that happens sometimes -}
: BasicExpr  { Expr { expr=($1 :: ScalarExpr ), alias=Nothing } }
| BasicExpr as QualifiedName { Expr { expr=($1 :: ScalarExpr ), alias= Just $3 } }

{-attributes only seem to show up next to column refernces, and sometimes functions -}
BasicExpr
: BasicExprBare  { $1 :: ScalarExpr }

AttrList
: { [] }
| Attr AttrList { $1 : $2 }

Attr
{- NOTNULL shows up after columns marked NOT NULL in the schena, and sometimes function calls like sys.sum() -}
: NOTNULL { NotNull }
{- ASC shows up in 2nd bracket list of projects, after column references when ORDER BY is in the query
Desc does not produce an explicit annotation.-}
| ASC { Asc }
{- HASHCOL shows up next to column references, those that are primary keys -}
| HASHCOL { HashCol }
{- JOINIDX the name refers to a fk constraint name (which looks like a column name) in the schema on the right -}
| JOINIDX QualifiedName { JoinIdx $2 }
{- HASHIDX shows up on table() operators. foreign key constraint name on the left-}
| HASHIDX { HashIdx }
{- shows up in some join bracket lists -}
| FETCH { Fetch }

{- expression with no attributes yet.
  Almost always used followed by attributes, so use BasicExpr instead of this.
-}
BasicExprBare
: QualifiedName AttrList  { Ref $1 $2 }
| QualifiedName '(' ExprList ')' AttrList -- not null and hashcol sometimes
  { Call { fname = $1, args = $3 } }
| QualifiedName notnil '(' ExprList ')' AttrList
  { Call { fname = $1, args = $4 } }
| TypeSpec '[' Expr ']' { Cast { tspec=$1, value=$3 } }
| TypeSpec literal  { Literal {tspec=$1
                             ,stringRep = reverse $ tail $ reverse $ tail $2
                             }
                   }
| FilterExpr { $1 }
| InExpr { $1 }
| '(' ExprListNE ')' { Nested $2 }

{- for now, only seen like after a char[] cast, which is a basic expression.
 note that like is also used in sys.like, so treating it specially breaks things
-}
FilterExpr
:  ExprBind FILTER identifier '(' Expr ',' BasicExprBare ')' {- it should be a 2 elt list, last one being a literal -}
  { Filter { arg = $1, oper=$3, negated = False, pattern=$5, escape=$7 } }
|  ExprBind '!' FILTER identifier '(' Expr ',' BasicExprBare ')' {- it should be a 2 elt list, last one being a literal -}
  { Filter { arg = $1, oper = $4, negated = True, pattern=$6, escape=$8 } }

{- for now, only seen IN after a column ref (with NOT NULL in it), and after
some function call. the internal expr is a comma infix -}
InExpr
: ExprBind in '(' ExprList ')' { In { arg = $1, negated = False, set = $4 } }
| ExprBind notin '(' ExprList ')' { In { arg = $1, negated = True, set = $4 } }

----------------------------------- Haskell -----------------------------------
{

{- eg decimal(15,2) , or smallint  -}
data TypeSpec = TypeSpec { tname :: String
                         , tparams :: [Int] } deriving (Eq,Show,Generic)

instance NFData TypeSpec

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

data Expr = Expr { expr :: ScalarExpr, alias :: Maybe Name } deriving (Eq,Show,Generic)
instance NFData Expr

data Attr = NotNull
          | JoinIdx Name
          | HashCol
          | HashIdx
          | Asc
          | Fetch deriving (Eq,Show,Generic)
instance NFData Attr

{- todo: deal with things like x < y < z that show up in the select args-}
data ScalarExpr =  Literal { tspec :: TypeSpec
                           , stringRep :: String
                           }
                   | Ref   { rname :: Name, attrs::[Attr] }
                   | Call  { fname :: Name
                           , args :: [Expr]
                           }
                   | Cast  { tspec :: TypeSpec
                           , value :: Expr
                           }
                   | Infix { infixop :: String
                           , left :: Expr
                           , right :: Expr
                           }
                   | Interval { ifirst :: Expr
                              , firstop::String
                              , imiddle::Expr
                              , secondop::String
                              , ilast::Expr
                              }
                   | Filter{ arg :: Expr
                           , oper :: String {-eg like, or ilike or iregex -}
                           , negated :: Bool
                           , pattern :: Expr
                           , escape :: ScalarExpr
                           }
                   | In    { arg :: Expr
                           , negated :: Bool
                           , set :: [Expr]  {- alias should probably be Nothing -}
                           }
                   | Nested [Expr] {-its here just allow types to check without modifying the grammar -}

                   deriving (Eq, Show, Generic)
instance NFData ScalarExpr

data Rel = Node { relop :: String {- relational op like join -}
                , children :: [Rel]
                , arg_lists :: [[Expr]]  }
           | Leaf { source :: Name, columns :: [Expr] }
             {-table scan -}
           deriving (Eq, Show, Generic)
instance NFData Rel

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
  do tokens  <- sequence $ Scanner.scan str
     let parsetree  = parse tokens
     -- let tr = case parsetree  of
     --          Left err -> "\n--Error at Parser stage:\n" ++ err
     --          Right g ->  "\n--Parser output:\n" ++ groom g
     -- trace tr parsetree
     parsetree
}
