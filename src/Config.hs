module Config ( Config(..)
              , ColInfo(..)
              , checkColInfo
              , makeConfig
              , isPkey
              , isFKRef
              , BoundsRec
              , WhichIsLeft(..)
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
--import qualified Data.Set as Set
import Data.List.NonEmpty(NonEmpty(..))
import qualified Data.List.NonEmpty as N
import qualified Data.ByteString.Lazy as B
--import qualified Data.ByteString.Char8 as C
type Map = Map.Map
--type Set = Set.Set

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
     let allpkeys = map makePKeys tables -- sort the non-empty lists
     return $ Config { grainsizelg, colinfo, fkrefs=Map.fromList allrefs, pkeys=Map.fromList allpkeys }

concatName :: Name -> Name -> Name
concatName (Name a) (Name b) = Name (a ++ b)

makePKeys :: Table -> (NonEmpty Name, Name)
makePKeys Table { pkey=FKey {} }  = error "fkey in place of pkey"
makePKeys Table { name, pkey=PKey { pkcols, pkconstraint=_ } } = (N.sort (N.map (concatName name) pkcols), name)

data WhichIsLeft = FactIsLeft | DimIsLeft deriving (Show,Eq,Generic)

makeFKEntries :: Table -> [(NonEmpty (Name, Name), (WhichIsLeft, Name))]
makeFKEntries Table { name, fkeys } =
  do FKey { references, colmap, fkconstraint } <- fkeys
     let (local,remote) = N.unzip colmap
     let localcols = fmap (concatName name) local
     let remotecols = fmap (concatName references) remote
     let joinidx = concatName name fkconstraint
     let implicit = N.sort (N.zip localcols remotecols) -- sort for canonical order
     let implicit_back = N.sort (N.zip remotecols localcols)
     let tidname = concatName references (Name ["%TID%"])
     let explicit = ( joinidx
                    , tidname ) :|[]
     let explicit_back = (tidname, joinidx) :| []
     [ (implicit, (FactIsLeft, joinidx))
       , (implicit_back, (DimIsLeft, joinidx))
       , (explicit, (FactIsLeft, joinidx))
       , (explicit_back, (DimIsLeft, joinidx)) ]

data Config =  Config  { grainsizelg :: Integer -- log of grainsizfae
                       , colinfo :: NameTable ColInfo
                       , fkrefs :: Map (NonEmpty (Name,Name)) (WhichIsLeft,Name)
                                   -- shows the fact -> dimension direction of the dependence
                       , pkeys :: Map (NonEmpty Name) Name
                                   -- fully qualifed column mames
                       }

-- if this list is a primary key, value is Just the table name, otherwise nothing
isPkey :: Config -> (NonEmpty Name) -> Maybe Name
isPkey conf nelist = let canon = N.sort nelist
                     in Map.lookup canon (pkeys conf)

-- if this list of pairs matches a known fk constraint, then the value shows is which is the dim/fact
-- and what the name of the constraint is
isFKRef :: Config -> (NonEmpty (Name,Name)) -> Maybe (WhichIsLeft, Name)
isFKRef conf cols = let canon = N.sort cols
                    in Map.lookup canon (fkrefs conf)

--- which queries do we want.
--- really: given colname
