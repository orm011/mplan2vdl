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

fromMplan :: M.RelExpr -> Either String [(Vexp, Maybe Name)]
fromMplan = solve

{- helper function that makes a lookup table from the output of a previous
operator. the lookup table loses information about the anonymous outputs
of the operator, so it only makes sense to use this in internal nodes
whose vectors will be consumed by other operators, but not on the
top level operator, which may return anonymous columns which we will need to
keep around -}
solve' :: M.RelExpr -> Either String (Map Name Vexp)
solve' relexp  = solve relexp >>= (return . makeEnv)
  where makeEnv lst = foldl maybeadd Map.empty lst
        maybeadd env (_, Nothing) = env
        maybeadd env (vexp, Just newalias) = Map.insert newalias vexp env

{- gets vexprs for the output columns of an operator, anonymous or not.
special case, because if the columns are not aliased, we can use their
original names.

note: not especially dealing with % names right now
todo: using the table schema we can resolve % names before
      they get to the final voodoo
-}
solve :: M.RelExpr -> Either String [ (Vexp, Maybe Name) ]
solve  M.Table { M.tablename, M.tablecolumns } =
  Right $ map deduceName tablecolumns
  where deduceName (orig, Nothing) = (External orig, Just orig)
        deduceName (orig, Just x) = (External orig, Just x)

{- not dealing with ordered queries right now
note. project affects the name scope in the following ways

There are four cases illustrated below:
project (...) [ foo, bar as bar2, sum(bar, baz) as sumbaz, sum(bar, bar) ]

For the consumer to be able to see all relevant columns, we need
to solve the cases like this:

1) despite not having an 'as' keyword, foo should produce
(Vexprfoo, Just foo)
2) for bar, the alias is explicit:
(Vexprbar, Just bar2)
3) for sum(bar, baz), the alias is also explicit. this is the same
as case 2 actually. (Vexpr sum(bar, baz), sum2)
4) sum(bar, bar) could not be referred to in a consumer query,
but could be the topmost result, so we must return it as well.
as (vexpr .., Nothing)
-}
solve M.Project { M.child, M.projectout, M.order = [] } =
  do env <- solve' child
     vexprs <- sequence $ map (fromScalar env) exprs
     let originalnames = map maybeGetName exprs
     let finalnames = map solveName $ zip originalnames finalnames
     return $ (zip vexprs finalnames)
  where maybeGetName (M.Ref orig) = Just orig
        maybeGetName _ = Nothing
        solveName (_, Just alias) = Just alias
        solveName (Just some, Nothing) = Just some
        solveName _ = Nothing
        (exprs, maliases) = unzip projectout


solve _ = Left "this relational operator is not supported yet"

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

fromPlanString :: String -> Either String [(Vexp, Maybe Name)]
fromPlanString mplanstring = M.fromString mplanstring >>= fromMplan
