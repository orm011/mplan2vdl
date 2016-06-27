module Config ( Config(..)
              , ColInfo(..)
              , checkColInfo
              , makeConfig
              , BoundsRec
              ) where

import Name as NameTable
--import Data.Int
import SchemaParser(Table(..),Key(..))
import qualified Data.Vector as V
import Control.Monad(foldM)
import Control.DeepSeq(NFData)
import GHC.Generics
import Control.Exception.Base
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import Data.List.NonEmpty(NonEmpty(..))
import qualified Data.List.NonEmpty as N
import qualified Data.ByteString.Lazy as B
--import qualified Data.ByteString.Char8 as C
type Map = Map.Map
type Set = Set.Set

-- we use integer in the rec so that strings get
-- parsed to Integer.
-- Parsing a string as an Int8, for example,  can silently
-- wraps the around the input string value and return an Int8
-- which is not the behavior we want here.
-- The same thing happens with wider types.
-- We want any type bound errors (such as being <  0 when it shouldnt) to
-- be the result of errors inherent in the input data, not of
-- errors due to overflwoing numbers within the program.
type BoundsRec = (B.ByteString, B.ByteString, Integer, Integer, Integer)

-- we use Integer values here so that our metadata calculations
-- don't overflow.
data ColInfo = ColInfo
  { bounds::(Integer,Integer)
    -- bounds on element values
  , count::Integer
    --this is a bound on the array size (num elts) needed to hold it
  } deriving (Eq,Show,Generic)

instance NFData ColInfo

checkColInfo :: ColInfo -> ColInfo
checkColInfo i@(ColInfo {bounds=(l,u), count}) = assert (l <= u && count > 0) i

addEntry :: NameTable ColInfo -> BoundsRec -> Either String (NameTable ColInfo)
addEntry nametab (tab,col,colmin,colmax,colcount) =
  let colinfo = checkColInfo $ ColInfo { bounds=(colmin, colmax), count=colcount }
  in NameTable.insert (Name [tab,col]) colinfo nametab

makeConfig :: Integer -> V.Vector BoundsRec -> [Table] -> Either String Config
makeConfig grainsizelg boundslist tables =
  do colinfo <- foldM addEntry NameTable.empty boundslist
     let allrefs = foldMap makeFKEntries tables
     let allpkeys = foldMap makePKeys tables
     return $ Config { grainsizelg, colinfo, fkrefs=Map.fromList allrefs, pkeys=Set.fromList allpkeys }

concatName :: Name -> Name -> Name
concatName (Name a) (Name b) = Name (a ++ b)

makePKeys :: Table -> [Name]
makePKeys Table { pkey=FKey {} }  = error "fkey in place of pkey"
makePKeys Table { name, pkey=PKey { pkcols=c0 :| [], pkconstraint=_ } } = [ concatName name c0 ]

makePKeys Table { pkey=PKey{ pkcols=_ :| (_:_), pkconstraint=_ }} = []

makeFKEntries :: Table -> [(NonEmpty (Name, Name), (NonEmpty (Name, Name), Name))]
makeFKEntries Table { name, fkeys } =
  do FKey { references, colmap, fkconstraint } <- fkeys
     let (local,remote) = N.unzip colmap
     let localcols = fmap (concatName name) local
     let remotecols = fmap (concatName references) remote
     let joinidx = concatName name fkconstraint
     let implicit = N.zip localcols remotecols
     let implicit_back = N.zip remotecols localcols
     let tidname = concatName references (Name ["%TID%"])
     let explicit = ( joinidx
                    , tidname ) :|[]
     let explicit_back = (tidname, joinidx) :| []
     [ (implicit, (implicit,joinidx))
       , (implicit_back, (implicit,joinidx))
       , (explicit, (explicit, joinidx))
       , (explicit_back, (explicit, joinidx)) ]

data Config =  Config  { grainsizelg :: Integer -- log of grainsizfae
                       , colinfo :: NameTable ColInfo
                       , fkrefs :: Map (NonEmpty (Name,Name)) (NonEmpty(Name,Name),Name)
                                   -- shows the fact -> dimension direction of the dependence
                       , pkeys :: Set Name
                                   -- fully qualifed column mames
                       }

--- which queries do we want.
--- really: given colname
