module Config (Config(..)
              ) where

import Data.Int

data Config =  Config  { grainsizelg :: Int64
                       } deriving (Show,Eq)
