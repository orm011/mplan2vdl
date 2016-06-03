module Config (Config(..)
              ) where

import Data.Typeable
import Data.Data

data Config =  Config  { mplanfile :: String
                       , grainsize :: Int
                       } deriving (Show, Data, Typeable)
