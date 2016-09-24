module Types( MType(..)
            , SType(..)
            , DType(..)
            , resolveTypeSpec
            , sizeOf
            , bitwidthOf
            , getSTypeOfMType
            , boundsOf
            , withinBounds
            ) where

import Name as NameTable

--import Data.Char
import Data.Data
import Data.Hashable
import Data.Int

import GHC.Generics
--import qualified Data.ByteString.Lazy.Char8 as C

import Control.DeepSeq(NFData)
--- repr info from Monet tpch001
-- sql>select distinct "type","columnsize"/"count" as rep_size from storage where "schema"='sys' and ("table"='lineitem' or "table"='orders' or "table"='customer' or "table"='partsupp' or "table"='supplier' or "table"='part' or "table"='nation' or "table"='region') order by "type","rep_size";
-- +---------+----------+
-- | type    | rep_size |
-- +=========+==========+
-- | char    |        1 |
-- | char    |        2 |
-- | date    |        4 |
-- | decimal |        8 |
-- | int     |        4 |
-- | oid     |        8 |
-- | varchar |        2 |
-- | varchar |        4 |
-- +---------+----------+

--- repr info from Monet tpch10
-- select distinct "type","columnsize"/"count" as rep_size from storage where "schema"='sys' and ("table"='lineitem' or "table"='orders' or "table"='customer' or "table"='partsupp' or "table"='supplier' or "table"='part' or "table"='nation' or "table"='region') order by "type","rep_size";
-- +---------+----------+
-- | type    | rep_size |
-- +=========+==========+
-- | char    |        1 |
-- | char    |        2 |
-- | char    |        4 |
-- | date    |        4 |
-- | decimal |        8 |
-- | int     |        4 |
-- | oid     |        8 |
-- | varchar |        2 |
-- | varchar |        4 |
-- +---------+----------+

-- these are storage types used in monet tpch.
-- for different tables, other types may show up.
--- right now, we immediately convert literals to the types they are used with
-- (eg, dates, millisecs, months to days.)
-- Date gets stored as integer startig from 0 (4 bytes)
-- Char and Varchar get treated the same way here.
-- Decimal is needed here in order to correctly do conversions between decimals.
--Precision is the number of digits in a number.
-- Scale is the number of digits to the right of the decimal point in a number.
-- For example, the number 123.45 has a precision of 5 and a scale of 2.
-- limited to the storage types (and also the C types in the backend)
data SType  =
  SDecimal {precision::Integer, scale::Integer}  -- as an integer?. 1<=P<=18. 0<=S<=P
  | SInt32 -- 4 bytes.
  | SInt64 -- 8 bytes.
  deriving (Eq,Show,Generic,Data)
instance Hashable SType
instance NFData SType

-- as an integer?. 1<=P<=18. 0<=S<=P. -- useful in order to understand
-- how to display the results (also how to do decimal arithmetic)
data DType = -- higher level types (eg dates etc)
  DDecimal {point::Int} -- position of the decimal point (0 is equivalent to an integer)
  | DString {dict::Name} -- name of the dictionary needed to decode this encoded string
  | DDate
  deriving (Eq,Show,Generic,Data)
instance Hashable DType

sizeOf :: SType -> Integer
sizeOf (SDecimal {}) = 8
sizeOf SInt32 = 4
sizeOf SInt64 = 8
bitwidthOf :: SType -> Integer
bitwidthOf t = 8 * sizeOf t

boundsOf :: SType -> (Integer, Integer)
boundsOf stype = case stype of
  SDecimal {} -> ( toInteger (minBound :: Int64 )
                 , toInteger (maxBound :: Int64 ) )
  SInt32 -> ( toInteger (minBound :: Int32 )
            , toInteger (maxBound :: Int32 ) )
  SInt64 -> ( toInteger (minBound :: Int64 )
            , toInteger (maxBound :: Int64 ) )

withinBounds :: (Integer,Integer) -> SType -> Bool
withinBounds (l,u) stype =
  let (ll,uu) = boundsOf stype
  in (ll <= l && l <= u && u <= uu)

-- these are more general monet types that show
-- up as literals in tpch plans, but not as columns in storage.
-- we convert them as soon as possible to the stypes that fit them
-- so that we only need to think about fewer types.
data MType =
  MTinyint
  | MInt
  | MBigInt
  | MSmallint
  | MDate
  | MMillisec
  | MMonth
  | MDouble
  | MOid
  | MChar Integer
  | MVarchar Integer
  | MDecimal Integer Integer
  | MSecInterval Integer
  | MMonthInterval
  | MBoolean
  deriving (Eq, Show, Generic, Data)
instance Hashable MType
instance NFData MType

getSTypeOfMType :: MType -> SType
getSTypeOfMType mtype = case mtype of
  MInt -> SInt32
  MDate -> SInt32
  MOid -> SInt64
  MChar _ -> SInt64
  MVarchar _ -> SInt64
  MDecimal precision scale  -> SDecimal {precision, scale}
  MSmallint -> SInt32
  MTinyint -> SInt32
  MBigInt -> SInt64
  ow -> error $ "we don't expect reading this type from the monet columns/queries at the moment: " ++ (show ow)


resolveTypeSpec :: TypeSpec -> MType
resolveTypeSpec TypeSpec { tname, tparams } = f tname tparams
  where f "int" [] = MInt
        f "tinyint" [] = MTinyint
        f "smallint" [] = MSmallint
        f "bigint" [] = MBigInt
        f "date" []  = MDate
        f "char" [len] =  MChar len
        f "char" [] = MChar (-1) -- beh
        f "varchar" [maxlength] =  MVarchar maxlength
        f "decimal" [precision,scale] = MDecimal precision scale
        f "sec_interval" [_] = MMillisec -- they use millisecs to express their seconds
        f "month_interval" [] = MMonth
        f "double" [] = MDouble -- used for averages even if columns arent doubles
        f "boolean" [] = MBoolean
        f "oid" [] = MOid -- used in storage files
        -- capitalized forms come from schema file.
        f "INTEGER" [] = MInt
        f "CHAR" [len] = MChar len
        f "DECIMAL" [precision,scale] = MDecimal precision scale
        f "VARCHAR" [maxlength] = MVarchar maxlength
        f "DATE" [] = MDate
        f name _ = error $ "unsupported typespec: " ++ show name
