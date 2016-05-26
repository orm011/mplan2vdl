module Vlite( fromMplan
            , fromString) where

import qualified Mplan as M
import Mplan(BinaryOp)
import Name(Name(..))
import qualified Name as NameTable
import Control.Monad(foldM)
import Prelude hiding (lookup) {- confuses with Map.lookup -}
import GHC.Generics
import Control.DeepSeq(NFData)

import Debug.Trace
import Text.Groom

type NameTable = NameTable.NameTable

data ShOp = Gather | OpScatter
  deriving (Eq, Show, Generic)
instance NFData ShOp

data FoldOp = FSum | FMax | FMin
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
  | Fold { foldop :: FoldOp, fdata :: Vexp, fgroups :: Vexp }
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
solve' :: M.RelExpr -> Either String (NameTable Vexp)
solve' relexp  = solve relexp >>= makeEnv
  where makeEnv lst = foldM maybeadd NameTable.empty lst
        maybeadd env (_, Nothing) = Right env
        maybeadd env (vexp, Just newalias) = NameTable.insert newalias vexp env

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

{- the easy group by case: static single group -}
solve M.GroupBy { M.child,
                M.inputkeys = [],
                M.outputkeys = [],
                M.outputaggs } =
  do env <- solve' child
     let (aggs, aliases) = unzip outputaggs
     resolvedAggs <- sequence $ map (solveAgg env) aggs
     return $ zip resolvedAggs aliases
     where
       solveAgg env (M.Sum exp) = do fdata <- sc env exp
                                     return $ Fold { foldop = FSum,
                                                     fdata,
                                                     fgroups = zeros_ }
       solveAgg _ M.Count = return $ Fold { foldop = FSum,
                                 fdata = ones_,
                                 fgroups = zeros_ }
       solveAgg _ s_ = Left $ "unsupported aggregate: " ++ groom s_



{-direct, foreign key join of two tables.
for now, this join assumes but does not check
that the two tables have a fk dependency between them
-}
solve M.Join { M.lchild = M.Table _ _,
               M.rchild = M.Table _ _,
               M.condition = M.Binop { M.binop=M.Eq
                                , M.left=M.Ref leftcol
                                , M.right=M.Ref rightcol
                                }
             } = Left $ "under construction"
  -- do leftenv <- solve' lchild
  --    rightenv <- solve' rchild
  --    TODO check table lchild has leftcol_rightcol fk, get that column


solve (M.Join _ _ _) = Left $ "unsupported join (only fk-like joins right now)"

solve r_  = Left $ "unsupported M.rel:  " ++ groom r_

{- makes a vector from a scalar expression, given a context with existing
defintiions -}
fromScalar ::  NameTable Vexp -> M.ScalarExpr  -> Either String Vexp
fromScalar = sc

{- notes about tmp naming in Monet:
 -user columns are only named with lowercase.
 -temporary columns are sometimes named things like L.L or L1.L1.
 -to the right of 'as', we get fully qualified names
 -but sometimes, as a reference, they don't include the fully qualified names.
eg [L1 as L1]. This means we cannot just use a map in those cases, since
a search for L1 should potentially mean L1.L1.
-}

sc ::  NameTable Vexp -> M.ScalarExpr -> Either String Vexp
sc env (M.Ref refname)  =
  case (NameTable.lookup refname env) of
    Right (_, v) -> Right v
    Left s -> Left s


-- TODO: strictly speaking, I need to know the orignal type in order
-- to know What kind of computation is actually involved.
-- Also, voodoo does not have support to express wider /narrower types.
-- For now, make casting data into a noop
sc env (M.Cast { M.mtype, M.arg }) =
  case mtype of
    M.MTinyint -> sc env arg
    M.MInt -> sc env arg
    M.MBigInt -> sc env arg
    M.MSmallint -> sc env arg
    othertype -> Left $ "unsupported type cast: " ++ groom othertype

sc _ r = Left $ "(Vlite) unsupported M.scalar: " ++ groom r

-- string means monet plan string.
fromString :: String -> Either String [(Vexp, Maybe Name)]
fromString mplanstring =
  do mplan <- M.fromString mplanstring
     let vlite = fromMplan mplan
     let tr = case vlite of
                Left err -> "\n--Error at Vlite stage:\n" ++ err
                Right g -> "\n--Vlite output:\n" ++ groom g
     trace tr vlite
