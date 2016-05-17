module Vlite( fromMplan
            , fromPlanString) where

import qualified Mplan as M
import Mplan(BinaryOp, Name)

import qualified Data.Map.Strict as Map
import Data.Map.Strict((!))
import Prelude hiding (lookup) {- confuses with Map.lookup -}

type Map = Map.Map

data ShOp = Gather | OpScatter
  deriving (Eq, Show)

data FoldOp = Sum | Max | Min
  deriving (Eq, Show)

{- Range has no need for count arg at this point.
may convert to differnt range after -}
data Vexp =
  External Name
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

fromMplan :: M.RelExpr -> Either String (Map Name Vexp)
fromMplan = solve

solve  :: M.RelExpr -> Either String (Map Name Vexp)

solve  M.Table { M.tablename, M.tablecolumns } =
  let addElt m (orig, malias)  =
         {- note: not especially dealing with % names right now -}
        let export = case malias of
                       Nothing -> orig {-exports the same name-}
                       Just alias -> alias
        in Map.insert export (External orig) m
   in Right $ foldl addElt Map.empty tablecolumns

{- not dealing with orderd queries right now -}
solve M.Project { M.child, M.projectout, M.order = [] } =
  Left "todo"
  -- let prev = helper child
  --   in

solve _ = Left "this relational operator is not supported yet"

{-
takes a single scalar expression, and a set of existing bindings (name, vexp)
bindings and computes a Vexp for the input expression, possibly
in terms of expressions already in the context
-}

fromScalar ::  Map Name Vexp -> M.ScalarExpr  -> Either String Vexp
fromScalar = sc

sc ::  Map Name Vexp -> M.ScalarExpr -> Either String Vexp
sc env (M.Ref refname)  =
  case (Map.lookup refname env) :: Maybe Vexp of
   Nothing -> Left $ "no column named" ++ show refname ++ "within scope"
   Just v -> Right v

sc _ _ = Left "this scalar expr is not supported yet"

fromPlanString :: String -> Either String (Map Name Vexp)
fromPlanString mplanstring = M.fromString mplanstring >>= fromMplan
