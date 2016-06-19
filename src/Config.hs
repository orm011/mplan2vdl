module Config ( Config(..)
              , ColInfo(..)
              , makeConfig
              , BoundsRec
              ) where

import Name as NameTable
import Data.Int
import SchemaParser(Table(..))
import qualified Data.Vector as V
import Control.Monad(foldM)
import Control.DeepSeq(NFData)
import GHC.Generics

type BoundsRec = (String, String, Int64, Int64, Int64)

data ColInfo = ColInfo { bounds::(Int64,Int64) -- bounds on element values
                       , count::Int64 --this is a bound on the array size (num elts) needed to hold it
                       } deriving (Eq,Show,Generic)
instance NFData ColInfo

addEntry :: NameTable ColInfo -> BoundsRec -> Either String (NameTable ColInfo)
addEntry nametab (tab,col,colmin,colmax,colcount) =
  NameTable.insert (Name [tab,col]) ColInfo { bounds=(colmin, colmax), count=colcount } nametab

makeConfig :: Int64 -> V.Vector BoundsRec -> [Table] -> Either String Config
makeConfig grainsizelg boundslist tables =
  do colinfo <- foldM addEntry NameTable.empty boundslist
     return $ Config { grainsizelg, colinfo, tables }


data Config =  Config  { grainsizelg :: Int64 -- log of grainsize
                       , colinfo :: NameTable ColInfo
                       , tables:: [Table]
                       }

--- which queries do we want.
-- really: given colname.
