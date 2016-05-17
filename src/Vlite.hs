module Vlite( fromMplan
            , fromPlanString) where

import qualified Mplan as Mp
import Mplan(BinaryOp, Name)

data ShOp = Gather | OpScatter
  deriving (Eq, Show)

data FoldOp = Sum | Max | Min
  deriving (Eq, Show)

{- Range has no need for count arg at this point.
may convert to differnt range after -}
data Vexp =
  External {  ename :: [Name] }
  | Range  { rmin :: Int, rstep :: Int }
  | Binop { bop :: BinaryOp, bleft :: Vexp, bright :: Vexp }
  | Shuffle { shop :: ShOp,  shsource :: Vexp, shpos :: Vexp  }
  | FoldSel  { fsdata :: Vexp, fsgroups :: Vexp }
  | Fold { fop :: FoldOp, fdata :: Vexp, fgroups :: Vexp }
  | Cross
  | Partition
  | Cast
  deriving (Eq,Show)

{- some convenience vectors -}
const_ :: Int -> Vexp
const_ c = Range { rmin = c, rstep = 0 }

pos_ = Range { rmin = 0, rstep = 1 }
ones_ = const_ 1
zeros_ = const_ 0

fromMplan :: Mp.RelExpr -> Either String [Vexp]
fromMplan _ = Left "not implemented"

fromPlanString :: String -> Either String [Vexp]
fromPlanString mplanstring = Mp.fromString mplanstring >>= fromMplan
