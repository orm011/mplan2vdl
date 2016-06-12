-- -*- haskell -*-
{
module TreeParser ( parse
                  , fromString
                  , TRel(..)
                  ) where

import Text.Printf (printf)
import Data.Either(isRight,partitionEithers)
import Scanner (ScannedToken(..), Token(..), scan)
import Control.DeepSeq(NFData)
import GHC.Generics (Generic)
import Text.Groom
import Debug.Trace
import Data.String.Utils(join)
import Config

import Name(Name(..))
}

--------------------------------- Directives ----------------------------------

%name parse TProgram
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
  NOTNULL    { ScannedToken _ _ ( Word "NOT NULL") }
  HASHCOL    { ScannedToken _ _ ( Word "HASHCOL") }
  literal    { ScannedToken _ _ ( ValueLiteral $$) }
  number     { ScannedToken _ _ ( NumberLiteral $$) }
  COUNT      { ScannedToken _ _ ( Word "COUNT") }
  table      { ScannedToken _ _ ( Word "table") }
  identifier { ScannedToken _ _ ( Word $$  )  }


%% -------------------------------- Grammar for rel only ------------------------

TProgram: TTree { $1}

TTree
: TLeaf { $1 }
| TNode { $1 }

TLeaf
: table '(' QualifiedName ')' '[' TExt ']' COUNT
  { TLeaf { source=$3, columns=$6 }  }

QualifiedName
: QualifiedNameB { Name  $ dropsys $1 }

QualifiedNameB
: identifier { ( [$1] ) }
| identifier '.' QualifiedNameB { $1 : $3 }

TNode
: IdentifierListNE '(' TNodeListNE ')' TBracketListNE { TNode { relop = (join " " $1), children = $3, arg_lists = $5 } }

IdentifierListNE
: identifier { [$1] }
| identifier IdentifierListNE { $1 : $2 }

TNodeListNE
: TTree { ( $1 : [] ) :: [TRel] }
| TTree ',' TNodeListNE { ($1 : $3) :: [TRel] }

TBracketListNE
: '[' TExt ']' {  ($2 : []) :: [String] }
| '[' TExt ']' TBracketListNE  { ($2 : $4) :: [String] }

TExt
: { [] :: String }
| TExtAtom TExt { ($1 ++ " " ++ $2) :: String }
| QualifiedName TExt { (show $1 ++ " " ++ $2) }

TExtAtom -- just reassemble the tokens. dots belong in names
: literal { $1 }
| number { show $1 }
| ',' { show $ extractRawToken $1 }
| '(' { show $ extractRawToken $1 }
| ')' { show $ extractRawToken $1 }
| NOTNULL { "" }
| HASHCOL { "" }
| TNested { $1 }

TNested -- i think i need to deal with this case separately
: '[' TExt ']' { "[ " ++ $2 ++ " ]" }


----------------------------------- Haskell -----------------------------------
{

data TRel =
  TNode { relop :: String
        , children :: [TRel]
        , arg_lists :: [String]  }
  | TLeaf { source :: Name, columns :: String }
  deriving (Eq,Show,Generic)


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

fromString :: String -> Config -> Either String TRel
fromString str cfg  =
  let tokens = Scanner.scan str
      in parse tokens

-- drop the 'sys' prefix that shows up some times (but not all of the time)
dropsys :: [String] -> [String]
dropsys ("sys":rest) = rest
dropsys x = x

}
