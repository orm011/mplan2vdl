module Voodoo (Voodoo(..)
               , fromString
               ,Vref(..)
               ) where

import Name(Name(..))
import Data.Int
import Debug.Trace
import Text.Groom
import GHC.Generics
import Data.String.Utils(join)
import Control.DeepSeq(NFData,($!!))
import qualified Vlite as V

{-
Aims to cover the subset of VoodooStdLib we need for Monet plans.
Nothing in this type should be non-existant in Voodoo.
Meant mostly so that it is easy to convert  to text format.
-}
data Voodoo =
  Load Name
  | Range { rmin::Int64, rstep::Int64 }
  | Binary { op::Voodop, arg1::Voodoo, arg2::Voodoo  }
  deriving (Eq,Show,Generic)
instance NFData Voodoo

data Voodop =
  LogicalAnd
  | LogicalOr
  | BitwiseAnd
  | BitwiseOr
  | Equals
  | Add
  | Subtract
  | Greater
  | Multiply
  | Divide
  | Gather
  | FoldMax
  | FoldSum
  | FoldMin
  | FoldCount
  deriving (Eq,Show,Generic)
instance NFData Voodop

{- the difference now is that
all pointers to other expressions now are explicit,
needed for serialization -}
data Vref  =
  VLoad Name
  | VRange  { vrmin :: Int64, vrstep :: Int64 }
  | VBinary { vop :: Voodop, varg1 :: Int, varg2 :: Int }
  deriving (Eq,Show,Generic)
instance NFData Vref

fromVexp :: V.Vexp -> Either String Voodoo
fromVexp _ = Left "implement me"

fromString :: String -> Either String [(Int, Vref)]
fromString str =
  do pairs <-  V.fromString str
     let (vexps, _) = unzip pairs
     vecs  <- mapM fromVexp vexps
     let log0 = [(0, VLoad $ Name ["dummy"])]
     return $ tail $ reverse $ foldl process log0 vecs
       where process log vec  = let (newl, _) = fromVoodoo log vec
                                in newl

addToLog :: [(Int, Vref)] -> Vref -> ([(Int, Vref)], Int)
addToLog log v = let (n, _) = head log
                 in ((n+1, v) : log, n+1)

fromVoodoo :: [(Int, Vref)] -> Voodoo -> ([(Int, Vref)], Int)

fromVoodoo log v@(Load n) = addToLog log (VLoad n)
fromVoodoo log v@(Range  { rmin , rstep  }) =
  addToLog log (VRange { vrmin=rmin, vrstep=rstep })

fromVoodoo log v@(Binary { op , arg1 , arg2 }) =
  let (newlog, newn) = fromVoodoo log arg1
      (newlog', newn') = fromVoodoo newlog arg2
      newbinop = VBinary {vop=op, varg1=newn, varg2=newn'}
      in addToLog newlog' newbinop

{- now a list of strings -}
toVcsv :: Vref -> [String]
toVcsv (VLoad n) = ["Load", show n]
toVcsv (VRange { vrmin, vrstep }) = ["Range", show vrmin, show vrstep]
toVcsv (VBinary { vop, varg1, varg2}) =
  [ printVoodop vop
  , "Id " ++ show varg1
  , "Id " ++ show varg2]

{- needs to be parsable by interpreter.h -}
printVoodop :: Voodop -> String
printVoodop op =
  case op of
  Greater -> "Greater"
  LogicalAnd -> "LogicalAnd"
  LogicalOr -> "LogicalOr"
  BitwiseAnd -> "BitwiseAnd"
  BitwiseOr -> "BitwiseOr"
  Equals -> "Equals"
  Add -> "Add"
  Subtract -> "Subtract"
  Greater -> "Greater"
  Multiply -> "Multiply"
  Divide -> "Divide"
  Gather -> "Gather"
  FoldMax -> "foldMax"
  FoldSum -> "foldSum"
  FoldMin -> "foldMin"
  FoldCount -> "foldCount"
