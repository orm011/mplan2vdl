module Config ( Config(..)
              , ColInfo(..)
              ) where

import Name
import Data.Int

data ColInfo = ColInfo { bounds::(Int64,Int64)
                       , count::Int64 -- needed for some bounds
                       } deriving (Eq,Show)

data Config =  Config  { grainsizelg :: Int64 -- log of grainsize
                       , colinfo :: NameTable ColInfo
                       }
