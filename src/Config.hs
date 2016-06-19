module Config ( Config(..)
              , ColInfo(..)
              , makeConfig
              , BoundsRec
              ) where

import Name as NameTable
--import Data.Int
import SchemaParser(Table(..))
import qualified Data.Vector as V
import Control.Monad(foldM)
import Control.DeepSeq(NFData)
import GHC.Generics


-- we use integer in the rec so that strings get
-- parsed to Integer.
-- Parsing a string as an Int8, for example,  can silently
-- wraps the around the input string value and return an Int8
-- which is not the behavior we want here.
-- The same thing happens with wider types.
-- We want any type bound errors (such as being <  0 when it shouldnt) to
-- be the result of errors inherent in the input data, not of
-- errors due to overflwoing numbers within the program.
type BoundsRec = (String, String, Integer, Integer, Integer)

-- we use Integer values here so that our metadata calculations
-- don't overflow.
data ColInfo = ColInfo
  { bounds::(Integer,Integer)
    -- bounds on element values
  , count::Integer
    --this is a bound on the array size (num elts) needed to hold it
  } deriving (Eq,Show,Generic)

instance NFData ColInfo

addEntry :: NameTable ColInfo -> BoundsRec -> Either String (NameTable ColInfo)
addEntry nametab (tab,col,colmin,colmax,colcount) =
  NameTable.insert (Name [tab,col]) ColInfo { bounds=(colmin, colmax), count=colcount } nametab

makeConfig :: Integer -> V.Vector BoundsRec -> [Table] -> Either String Config
makeConfig grainsizelg boundslist tables =
  do colinfo <- foldM addEntry NameTable.empty boundslist
     return $ Config { grainsizelg, colinfo, tables }


data Config =  Config  { grainsizelg :: Integer -- log of grainsize
                       , colinfo :: NameTable ColInfo
                       , tables:: [Table]
                       }

--- which queries do we want.
-- really: given colname.
