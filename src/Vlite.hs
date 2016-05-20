module Vlite( fromMplan
            , fromString) where

import qualified Mplan as M
import Mplan(BinaryOp, Name)
import Data.String.Utils(join)

import qualified Data.Map.Strict as Map
import Prelude hiding (lookup) {- confuses with Map.lookup -}
import GHC.Generics
import Control.DeepSeq(NFData)

import Debug.Trace
import Text.Groom

type Map = Map.Map

data ShOp = Gather | OpScatter
  deriving (Eq, Show, Generic)
instance NFData ShOp

data FoldOp = Sum | Max | Min
  deriving (Eq, Show, Generic)
instance NFData FoldOp

{- Range has no need for count arg at this point.
may convert to differnt range after -}
data Vexp =
  Load Name
  | Range  { rmin :: Int, rstep :: Int } {- length deduced by context later.
actually, we should just use the count as an input parameter (just like
we need max) -}
  | CRange { rmin :: Int, rstep :: Int, rmax :: Int } {- fixed length -}
  | Binop { bop :: BinaryOp, bleft :: Vexp, bright :: Vexp }
  | Shuffle { shop :: ShOp,  shsource :: Vexp, shpos :: Vexp  }
  | FoldSel  { fsdata :: Vexp, fsgroups :: Vexp }
  | Fold { fop :: FoldOp, fdata :: Vexp, fgroups :: Vexp }
  | Cross
  | Partition
  | Cast
  deriving (Eq,Show, Generic)
instance NFData Vexp

{- some convenience vectors -}
const_ :: Int -> Vexp
const_ c = Range { rmin = c, rstep = 0 }

pos_ = Range { rmin = 0, rstep = 1 }
ones_ = const_ 1
zeros_ = const_ 0

range_ n = CRange {rmin = 0, rstep = 1, rmax = n }

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

kMaxval :: Int
kMaxval = 255

solve :: M.RelExpr -> Either String [ (Vexp, Maybe Name) ]

{- Table is a Special case. It gets vexprs for all the output columns.

If the output columns are not aliased, we can always
use their original names as output names. (ie, there are no
anonymous expressions in the list)

note: not especially dealing with % names right now
todo: using the table schema we can resolve % names before
      they get to the final voodoo
-}
solve M.Table { M.tablename, M.tablecolumns } =
  Right $ map deduceName tablecolumns
  where deduceName (orig, Nothing) = (Load orig, Just orig)
        deduceName (orig, Just x) = (Load orig, Just x)

{- Project: not dealing with ordered queries right now
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
     return (zip vexprs finalnames)
  where maybeGetName (M.Ref orig) = Just orig
        maybeGetName _ = Nothing
        solveName (_, Just alias) = Just alias {-alias always wins-}
        solveName (Just some, Nothing) = Just some
        solveName _ = Nothing
        (exprs, maliases) = unzip projectout
        originalnames = map maybeGetName exprs
        finalnames = map solveName $ zip originalnames maliases

{- need to partition the inputs by first group values...
   then go on for a second group..
   partition one column then apply to other column.

   hardcoded assumptions: max value of columns.
   (TODO: plug this through as a configuration param, as well as counts)
-}

-- solve M.Group { M.child,
--                 M.inputkeys,
--                 M.outputkeys,
--                 M.outputaggs } = Left "implement me " ++


solve r_  = Left $ "(Vlite) unsupported M.rel:  " ++ groom r_

{- makes a vector from a scalar expression, given a context with existing
defintiions -}
fromScalar ::  Map Name Vexp -> M.ScalarExpr  -> Either String Vexp
fromScalar = sc

sc ::  Map Name Vexp -> M.ScalarExpr -> Either String Vexp
sc env (M.Ref refname)  =
  case (Map.lookup refname env) :: Maybe Vexp of
   Nothing -> Left $ "no column named " ++ (join "." refname) ++ " within scope"
   Just v -> Right v

sc _ r = Left $ "(Vlite) unsupported M.scalar: " ++ groom r

-- string means monet plan string.
fromString :: String -> Either String [(Vexp, Maybe Name)]
fromString mplanstring =
  do mplan <- M.fromString mplanstring
     let vlite = fromMplan mplan
     let tr = case vlite of
                Left err -> "Error at Vlite stage:\n" ++ err
                Right g -> "Vlite output\n: " ++ groom g
     trace tr vlite
