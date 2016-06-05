module Config ( Config(..)
              , ColInfo(..)
              ) where

import Name
import Data.Int

data ColInfo = ColInfo { lower::Int64
                       , upper::Int64
--                     , count::Int64 -- maybe add later
                       } deriving (Eq,Show)

data Config =  Config  { grainsizelg :: Int64 -- log of grainsize
                       , colinfo :: NameTable ColInfo
                       }
