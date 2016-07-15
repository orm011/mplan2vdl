module Config ( Config(..)
              , ColInfo(..)
              , checkColInfo
              , makeConfig
              , isPkey
              , isFKRef
              , lookupPkey
              , isPartialFk
              , isPartialPk
              , FKCols
              , PKCols
              , BoundsRec
              , StorageRec
              , FKJoinOrder(..)
              , MType(..)
              , resolveTypeSpec
              ) where

import Data.Data
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
import Data.Hashable
import qualified Data.ByteString.Lazy.Char8 as C
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

-- contents of select * from storage
type StorageRec = ( C.ByteString -- schema
                  , C.ByteString -- table
                  , C.ByteString -- column
                  , C.ByteString -- type
                  , C.ByteString -- location
                  , Integer -- count
                  , Integer -- typewidth (bytes)
                  , Integer -- columnsize (bytes)
                  , Integer -- heapsize
                  , Integer -- hashes
                  , Integer -- imprints
                  , C.ByteString  -- sorted (boolean)
                  )

data MType =
  MTinyint
  | MInt
  | MBigInt
  | MSmallint
  | MOid
  | MDate
  | MMillisec
  | MMonth
  | MDouble
  | MChar Integer
  | MVarchar Integer
  | MDecimal Integer Integer
  | MSecInterval Integer
  | MMonthInterval
  | MBoolean
  deriving (Eq, Show, Generic, Data)
instance NFData MType

resolveTypeSpec :: TypeSpec -> MType
resolveTypeSpec TypeSpec { tname, tparams } = f tname tparams
  where f "int" [] = MInt
        f "tinyint" [] = MTinyint
        f "smallint" [] = MSmallint
        f "bigint" [] = MBigInt
        f "date" []  = MDate
        f "char" [len] =  MChar len
        f "varchar" [maxlength] =  MVarchar maxlength
        f "decimal" [precision,scale] = MDecimal precision scale
        f "sec_interval" [_] = MMillisec -- they use millisecs to express their seconds
        f "month_interval" [] = MMonth
        f "double" [] = MDouble -- used for averages even if columns arent doubles
        f "boolean" [] = MBoolean
        f "oid" [] = MOid -- used in storage files
        f "INTEGER" [] = MInt -- CAPS comes from parsed schema
        f "CHAR" [len] = MChar len
        f "DECIMAL" [precision,scale] = MDecimal precision scale
        f "VARCHAR" [maxlength] = MVarchar maxlength
        f "DATE" [] = MDate
        f name _ = error $ "unsupported typespec: " ++ show name

toKeyPair :: (NameTable TypeSpec) -> StorageRec -> (Name, MType)
toKeyPair ns (_,tab,col,typstring,_,_,bytewidth,_,_,_,_,_) =
  let name = Name [tab,col]
      tspec = if typstring /= "oid"
              then snd $ (NameTable.lookup_err name ns)
              else TypeSpec { tname="oid", tparams=[] }
      mtype = resolveTypeSpec tspec
  in assert (case mtype of
                MChar _ -> True
                MVarchar _ -> True
                _ -> getTypeBitwidth mtype == (8*bytewidth)) (name,mtype)

-- holds the names of the matching columns for an fk.
-- the order matters, watch out. left is fact, right is dim.
type FKCols = NonEmpty (Name, Name)
type PKCols = NonEmpty Name

getTypeBitwidth :: MType -> Integer
getTypeBitwidth tp = case tp of
  MTinyint -> 8
  MInt -> 32
  MBigInt -> 64
  MDecimal _ _ -> 64
  MVarchar _ -> 64
  MChar _ -> 64 -- assuming that when dict encoding, offsets get stored as 64 bit ints.
  MOid -> 64
  MDate -> 32
  ow -> error $  "implement type bitwidth for " ++ show ow

-- we use Integer values here so that our metadata calculations
-- don't overflow.
data ColInfo = ColInfo
  { bounds::(Integer,Integer)
    -- bounds on element values
    --this is a bound on the array size (num elts) needed to hold it
  , count::Integer
  , coltype :: MType
  } deriving (Eq,Show,Generic)

instance NFData ColInfo

checkColInfo :: ColInfo -> ColInfo
checkColInfo i@(ColInfo {bounds=(l,u), count}) = assert (l <= u && count > 0) i

addEntry :: [Name] -> NameTable MType -> NameTable ColInfo -> BoundsRec -> Either String (NameTable ColInfo)
addEntry constraints storagetab nametab (tab,col,colmin,colmax,colcount) =
  do let coltype  = snd $ NameTable.lookup_err (Name [tab,col]) storagetab
     let colinfo = checkColInfo $ ColInfo { bounds=(colmin, colmax), count=colcount, coltype }
     plain <- NameTable.insert (Name [tab,col]) colinfo nametab
     if elem (Name[tab,col]) constraints
       then NameTable.insert (Name [tab, B.append "%" col]) colinfo plain -- constraints get marked with % as well
       else return $ plain

makeConfig :: Integer -> V.Vector BoundsRec -> Bool -> (V.Vector StorageRec) -> [Table] -> Either String Config
makeConfig grainsizelg boundslist shuffle_aggs storagelist tables =
  do let constraints = foldMap getTableConstraints tables
     let tspecs = NameTable.fromList $ foldMap getTspecs tables
     let storagemap = NameTable.fromList $ map (toKeyPair tspecs)  (V.toList storagelist)
     colinfo <- foldM (addEntry constraints storagemap) NameTable.empty boundslist
     let allrefs = foldMap makeFKEntries tables
     let straighten qual (a,b) = case qual of
           FactDim -> (a,b)
           DimFact -> (b,a)
     let make_partials (nelist,(qual,_)) = N.toList $ N.map (\n -> (n, (qual,N.map (straighten qual) nelist))) nelist
     let partialfks = Map.fromList $ foldMap make_partials allrefs
     let allpkeys = map makePKeys tables -- sort the non-empty lists
     let partialpks = Map.fromList $
           foldMap (\(pkl,_) -> map (\col -> (col,pkl)) (N.toList pkl)) allpkeys
     let pktable = map pkpair tables
     return $ Config { grainsizelg, colinfo, shuffle_aggs, fkrefs=Map.fromList allrefs, pkeys=Map.fromList allpkeys,
                       tablePKeys=Map.fromList pktable, partialfks, partialpks }

pkpair :: Table -> (Name,Name)
pkpair Table{name, pkey=PKey{pkconstraint}} = (name, concatName name pkconstraint)
pkpair _ = error "table with no pkey"

concatName :: Name -> Name -> Name
concatName (Name a) (Name b) = Name (a ++ b)

getTableConstraints :: Table -> [Name]
getTableConstraints Table {name, pkey, fkeys}
  = map (concatName name) (getConstraint pkey : map getConstraint fkeys)

getTspecs :: Table -> [(Name, TypeSpec)]
getTspecs tab = map (\(n,ts) -> (concatName (name tab) n, ts)) $ N.toList $ columns tab

getConstraint :: Key -> Name
getConstraint PKey {pkconstraint} = pkconstraint
getConstraint FKey {fkconstraint} = fkconstraint

makePKeys :: Table -> (NonEmpty Name, Name)
makePKeys Table { pkey=FKey {} }  = error "fkey in place of pkey"
makePKeys Table { name, pkey=PKey { pkcols, pkconstraint } } = (N.sort (N.map (concatName name) pkcols), concatName name pkconstraint)

data FKJoinOrder = FactDim | DimFact deriving (Show,Eq,Generic,Ord)
instance Hashable FKJoinOrder

makeFKEntries :: Table -> [(FKCols, (FKJoinOrder, Name))]
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
     let fkrefs = [ (implicit, (FactDim, joinidx))
                  , (implicit_back, (DimFact, joinidx))
                  , (explicit, (FactDim, joinidx))
                  , (explicit_back, (DimFact, joinidx)) ]
     fkrefs

data Config =  Config  { grainsizelg :: Integer -- log of grainsizfae
                       , colinfo :: NameTable ColInfo
                       , shuffle_aggs :: Bool
                       , fkrefs :: Map FKCols (FKJoinOrder,Name)
                                   -- shows the fact -> dimension direction of the dependence
                       , pkeys :: Map (NonEmpty Name) Name -- maps set of columns to pkconstraint if there is one
                                   -- fully qualifed column mames
                       , tablePKeys :: Map Name Name -- maps table to its pkconstraints
                       , partialfks :: Map (Name,Name) (FKJoinOrder, FKCols)
                       , partialpks :: Map Name  PKCols
                       } deriving (Show)

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
isFKRef :: Config -> FKCols -> Maybe (FKJoinOrder, Name)
isFKRef conf cols = let canon = N.sort cols
                    in Map.lookup canon (fkrefs conf)

isPartialFk :: Config -> (Name,Name) -> Maybe (FKJoinOrder, FKCols)
isPartialFk config (a,b) = Map.lookup (a,b) (partialfks config)


isPartialPk :: Config -> Name -> Maybe PKCols
isPartialPk config n = Map.lookup n (partialpks config)


--- which queries do we want.
--- really: given colname
