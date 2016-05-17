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
         {- todo: using the table schema we can resolve % names before
             they get to the final voodoo -}
        let export = case malias of
                       Nothing -> orig {-exports the same name-}
                       Just alias -> alias
        in Map.insert export (External orig) m
   in Right $ foldl addElt Map.empty tablecolumns

{- not dealing with ordered queries right now -}
solve M.Project { M.child, M.projectout, M.order = [] } =
  let (exprs, maliases) = unzip projectout
  in do env <- solve child
        vexprs <- sequence $ map (fromScalar env) exprs
        return $ foldl processBinding env (zip vexprs maliases)
        

solve _ = Left "this relational operator is not supported yet"

{- updates the env with a new binding -}
processBinding :: Map Name Vexp -> (Vexp, Maybe Name)  -> Map Name Vexp
{- case nothing: for anything other than the Table operator,
the name must already exist in the env, or otherwise the result won't be used again
within the expression (eg top-level project operators leave comples outputs anonymous sometimes)
TODO: could check, if the vexp is an External, that this binding exists in the scope -}
processBinding env (_, Nothing) = env
processBinding env (vexp, Just newalias) = Map.insert newalias vexp env


{- makes a vector from a scalar expression, given a context with existing
defintiions -}
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
