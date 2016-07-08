module Config ( Config(..)
              , ColInfo(..)
              , checkColInfo
              , makeConfig
              , isPkey
              , isFKRef
              , lookupPkey
              , isPartialFk
              , FKSpec
              , BoundsRec
              , WhichIsFact(..)
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

-- holds the names of the matching columns for an fk.
-- the order matters, watch out. left is fact, right is dim.
type FKSpec = NonEmpty (Name, Name)

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

addEntry :: [Name] -> NameTable ColInfo -> BoundsRec -> Either String (NameTable ColInfo)
addEntry constraints nametab (tab,col,colmin,colmax,colcount) =
  do let colinfo = checkColInfo $ ColInfo { bounds=(colmin, colmax), count=colcount }
     plain <- NameTable.insert (Name [tab,col]) colinfo nametab
     if elem (Name[tab,col]) constraints
       then NameTable.insert (Name [tab, B.append "%" col]) colinfo plain -- constraints get marked with % as well
       else return $ plain

makeConfig :: Integer -> V.Vector BoundsRec -> [Table] -> Either String Config
makeConfig grainsizelg boundslist tables =
  do let constraints = foldMap getTableConstraints tables
     colinfo <- foldM (addEntry constraints) NameTable.empty boundslist
     let allrefs = foldMap makeFKEntries tables
     let make_partials (nelist,(qual,_)) = N.toList $ N.map (\n -> (n, (qual,nelist))) nelist
     let partialfks = Map.fromList $ foldMap make_partials allrefs
     let allpkeys = map makePKeys tables -- sort the non-empty lists
     let pktable = map pkpair tables
     return $ Config { grainsizelg, colinfo, fkrefs=Map.fromList allrefs, pkeys=Map.fromList allpkeys,
                       tablePKeys=Map.fromList pktable, partialfks }

pkpair :: Table -> (Name,Name)
pkpair Table{name, pkey=PKey{pkconstraint}} = (name, concatName name pkconstraint)
pkpair _ = error "table with no pkey"

concatName :: Name -> Name -> Name
concatName (Name a) (Name b) = Name (a ++ b)

getTableConstraints :: Table -> [Name]
getTableConstraints Table {name, pkey, fkeys}
  = map (concatName name) (getConstraint pkey : map getConstraint fkeys)

getConstraint:: Key -> Name
getConstraint PKey {pkconstraint} = pkconstraint
getConstraint FKey {fkconstraint} = fkconstraint

makePKeys :: Table -> (NonEmpty Name, Name)
makePKeys Table { pkey=FKey {} }  = error "fkey in place of pkey"
makePKeys Table { name, pkey=PKey { pkcols, pkconstraint } } = (N.sort (N.map (concatName name) pkcols), concatName name pkconstraint)

data WhichIsFact = FactIsLeftChild | FactIsRightChild deriving (Show,Eq,Generic)

makeFKEntries :: Table -> [(FKSpec, (WhichIsFact, Name))]
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
     let fkrefs = [ (implicit, (FactIsLeftChild, joinidx))
                  , (implicit_back, (FactIsRightChild, joinidx))
                  , (explicit, (FactIsLeftChild, joinidx))
                  , (explicit_back, (FactIsRightChild, joinidx)) ]
     fkrefs

data Config =  Config  { grainsizelg :: Integer -- log of grainsizfae
                       , colinfo :: NameTable ColInfo
                       , fkrefs :: Map FKSpec (WhichIsFact,Name)
                                   -- shows the fact -> dimension direction of the dependence
                       , pkeys :: Map (NonEmpty Name) Name -- maps set of columns to pkconstraint if there is one
                                   -- fully qualifed column mames
                       , tablePKeys :: Map Name Name -- maps table to its pkconstraints
                       , partialfks :: Map (Name,Name) (WhichIsFact, FKSpec)
                       }

-- if this list is a primary key, value is Just the table name, otherwise nothing
isPkey :: Config -> (NonEmpty Name) -> Maybe Name
isPkey conf nelist = let canon = N.sort nelist
                     in Map.lookup canon (pkeys conf)

lookupPkey :: Config -> Name -> Name
lookupPkey config tab =
  let x = Map.lookup tab (tablePKeys config)
  in case x of
    Nothing -> error "every table has a pkey. no info for table loaded?"
    Just n -> n

-- if this list of pairs matches a known fk constraint, then the value shows is which is the dim/fact
-- and what the name of the constraint is
isFKRef :: Config -> FKSpec -> Maybe (WhichIsFact, Name)
isFKRef conf cols = let canon = N.sort cols
                    in Map.lookup canon (fkrefs conf)

isPartialFk :: Config -> (Name,Name) -> Maybe (WhichIsFact, FKSpec)
isPartialFk config (a,b) = Map.lookup (a,b) (partialfks config)

--- which queries do we want.
--- really: given colname
