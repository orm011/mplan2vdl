-- -*- haskell -*-
{

{-# LANGUAGE ScopedTypeVariables #-}
module SchemaParser ( parse
                    , fromString
                    , Table(..)
                    , Key(..)
                      ) where

import Text.Printf (printf)
import Data.Either(isRight,partitionEithers)
import Scanner (ScannedToken(..), Token(..), scan)
import Control.DeepSeq(NFData)
import GHC.Generics (Generic)
--import Text.Groom
--import Debug.Trace
--import Data.ByteString.Search(replace)
--import Data.List.Utils(join,replace)
import Name(Name(..),TypeSpec(..))
import Data.List.NonEmpty hiding (head,tail,take)
import Prelude hiding (zip)
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C
import Control.Exception.Base hiding (mask)
}

--------------------------------- Directives ----------------------------------

%name parse Schema
%error { parseError }
%monad { Either String }

%tokentype { ScannedToken }

%token
  '('        { ScannedToken _ _ LParen }
  ')'        { ScannedToken _ _ RParen }
  ','        { ScannedToken _ _ Comma }
  ';'        { ScannedToken _ _ Semi }
  '.'        { ScannedToken _ _ Dot }
  qidentifier  { ScannedToken _ _ ( ValueLiteral $$) }
  number     { ScannedToken _ _ ( NumberLiteral $$) }
  {- as long as the queries dont use these words within the columns names
or within table names, this should work alright -}
  CREATETABLE  { ScannedToken _ _ ( Word "CREATE TABLE") }
  NOTNULL    { ScannedToken _ _ ( Word "NOT NULL") }
  CONSTRAINT  { ScannedToken _ _ ( Word "CONSTRAINT") }
  PRIMARYKEY { ScannedToken _ _ ( Word "PRIMARY KEY") }
  FOREIGNKEY { ScannedToken _ _ ( Word "FOREIGN KEY") }
  REFERENCES { ScannedToken _ _ ( Word "REFERENCES") }
  SET { ScannedToken _ _ ( Word "SET") }
  SCHEMA { ScannedToken _ _ ( Word "SCHEMA") }


  {- anthing not matched  by the previous special words is dealt with as an identifier -}
  identifier { ScannedToken _ _ ( Word $$  )  }


%% -------------------------------- Grammar -----------------------------------

Schema
: SET SCHEMA QQualifiedName ';'  TableListNE {$5}

TableListNE
: Table { $1 : [] }
| Table TableListNE { $1 : $2 }

Table
: CREATETABLE QQualifiedName '(' ColumnListNE KeyListNE ')' ';'
  {
    let pkey :| fkeys = $5
    in Table { name=$2
             , columns=$4
             , pkey
             , fkeys
             }
  }

ColumnListNE
: Column { $1 :| [] }
| Column ColumnListNE { $1 <| $2 }

Column
: QQualifiedName TypeSpec Nullspec ',' { ($1, $2) }

Key
: CONSTRAINT QQualifiedName PRIMARYKEY KeySpec {
  PKey { pkcols=$4, pkconstraint = $2 } }
| CONSTRAINT QQualifiedName FOREIGNKEY KeySpec REFERENCES QQualifiedName KeySpec {
  let { locals = $4;
        remotes = $7;
        colmap = zip locals remotes }
  in FKey {fkconstraint=$2, references=$6, colmap }
  }

KeyListNE
: Key { $1 :| []  }
| Key ',' KeyListNE { $1 <| $3 }

KeySpec
: '(' QListNE ')' { $2 }

QListNE
: QQualifiedName { $1:|[] }
| QQualifiedName ',' QListNE { $1 <| $3 }

Nullspec
: {}
| NOTNULL {}

QQualifiedName
: QQualifiedNameBuilder { Name ( dropsys $1 )  }

QQualifiedNameBuilder
: qidentifier { ( [dropq $1] :: [B.ByteString]) --qiden }
| qidentifier '.' QQualifiedNameBuilder { ( (dropq $1) : $3) :: [B.ByteString] --qiden }

TypeSpec
: identifier   { TypeSpec { tname = $1, tparams=[]  } }
| identifier '(' NumberListNE ')' { TypeSpec { tname=$1, tparams=$3 } }

NumberListNE
: number { ($1 : []) :: [Integer] }
| number ',' NumberListNE { ($1 : $3) :: [Integer] }


 -------------------------------- Haskell ---------------------------------
{
-- data Nullity = NotNull | Nullable; -- ignoring
-- fk is local col made of remote table remote pkey local idx col

data Key = PKey {pkcols::NonEmpty Name, pkconstraint::Name}
         | FKey {references::Name, colmap::NonEmpty (Name,Name), fkconstraint::Name }
-- left in lineup means local.

data Table = Table { name::Name
                   , columns :: NonEmpty (Name, TypeSpec)
                   , pkey :: Key
                   , fkeys :: [Key]
                   }

parseError :: [ScannedToken] -> Either String a
parseError [] = Left "unexpected EOF"
parseError toks = Left $ printf ("At %d:%d. unexpected token%s (at most %d shown): '%s'." :: String) lineNo columnNo s numToks badTokenText
  where numToks = 10
        (s :: String)  = (if (not $ null $ tail toks) then "s" else "")
        firstBadToken = head toks
        lineNo = Scanner.line firstBadToken
        columnNo = Scanner.column firstBadToken
        badTokenText = concatMap ((++ "  ") . show . extractRawToken) (take numToks toks)

fromString :: B.ByteString -> Either String [Table]
fromString str  =
  let tokens = Scanner.scan str
  in parse tokens

dropq :: B.ByteString -> B.ByteString
dropq str = let t = B.tail str
                r = B.reverse t
                g' = B.tail r
            in assert( C.head str ==  '"' ) $
               assert (C.head r ==  '"') $
               B.reverse g'

-- drop the 'sys' prefix that shows up some times (but not all of the time)
dropsys :: [B.ByteString] -> [B.ByteString]
dropsys ("sys":rest) = rest
dropsys x = x

}
