module Vlite( vexpsFromMplan
            , Vexp(..)
            , BinaryOp(..)
            , ShOp(..)
            , FoldOp(..)) where

import Config
import qualified Mplan as M
import Mplan(BinaryOp(..))
import Name(Name(..))
import qualified Name as NameTable
import Control.Monad(foldM)
import Data.List (foldl')
import Prelude hiding (lookup) {- confuses with Map.lookup -}
import GHC.Generics
import Control.DeepSeq(NFData)
import Data.Int
--import Debug.Trace
import Text.Groom
--import qualified Data.Map.Strict as Map
--import Data.String.Utils(join)

--type Map = Map.Map
--type VexpTable = Map Vexp Vexp  --used to dedup Vexps

type NameTable = NameTable.NameTable

data ShOp = Gather | Scatter
  deriving (Eq, Show, Generic,Ord)
instance NFData ShOp

data FoldOp = FSum | FMax | FMin | FSel
  deriving (Eq, Show, Generic, Ord)
instance NFData FoldOp

{- Range has no need for count arg at this point.
may convert to differnt range after -}
data Vexp  =
  Load Name
  | Range  { rmin :: Int64, rstep :: Int64, rref::Vexp } -- reference Vector
  | RangeC { rmin :: Int64, rstep :: Int64, rcount::Int64 }
    -- ranges with fixed sizes
  | Binop { binop :: BinaryOp, left :: Vexp, right :: Vexp }
  | Shuffle { shop :: ShOp,  shsource :: Vexp, shpos :: Vexp }
  | Fold { foldop :: FoldOp, fgroups :: Vexp, fdata :: Vexp}
  | Partition { pivots::Vexp, pdata::Vexp }
  deriving (Eq,Show,Generic,Ord)
instance NFData Vexp


{- some convenience vectors -}
const_ :: Int64 -> Vexp -> Vexp
const_ k v = Range { rmin = k, rstep = 0, rref = v }

pos_ :: Vexp -> Vexp
pos_ v = Range { rmin = 0, rstep = 1, rref = v }

ones_ :: Vexp -> Vexp
ones_ = const_ 1

zeros_ :: Vexp -> Vexp
zeros_ = const_ 0

groups_ :: Int64 -> Vexp -> Vexp
groups_ groupsizelg v = (pos_ v) >>. (const_ groupsizelg v)

(==.) :: Vexp -> Vexp -> Vexp
a ==. b = Binop { binop=Eq, left=a, right = b}

(>>.) :: Vexp -> Vexp -> Vexp
a >>. b = Binop { binop=BitShift, left=a, right = b}

(||.) :: Vexp -> Vexp -> Vexp
a ||. b = Binop { binop=LogOr, left=a, right=b}

(-.) :: Vexp -> Vexp -> Vexp
a -. b = Binop { binop=Sub, left=a, right=b }

(*.) :: Vexp -> Vexp -> Vexp
a *. b = Binop { binop=Mul, left=a, right=b }

(+.) :: Vexp -> Vexp -> Vexp
a +. b = Binop { binop=Add, left=a, right=b }

-- --integer division
-- (/.) :: Vexp -> Vexp -> Vexp
-- a /. b = Binop { binop=Div, left=a, right=b }

(?.) :: Vexp -> (Vexp,Vexp) -> Vexp
cond ?. (a,b) = ((ones_ cond  -. negcond) *. a) +. (negcond *. b)
  where negcond = (cond ==. zeros_ cond)


vexpsFromMplan :: M.RelExpr -> Config -> Either String [(Vexp, Maybe Name)]
vexpsFromMplan r c  =
  do Env a _ <- solve c r
     return $ map pickfs a
     where pickfs (_1,_2,_) = (_1,_2)

-- the table only includes list elements that have a name
-- we also include metadata about the column, such as an
-- upper bound value in it.
data Env = Env [(Vexp, Maybe Name, ColInfo)] (NameTable (Vexp, ColInfo))

makeEnv :: [(Vexp, Maybe Name, ColInfo)] -> Either String Env
makeEnv lst =
  do tbl <- makeTable lst
     return $ Env lst tbl
  where makeTable pairs = foldM maybeadd NameTable.empty pairs
        maybeadd env (_, Nothing, _) = Right env
        maybeadd env (vexp, Just newalias, sz) =
          NameTable.insert newalias (vexp,sz) env

{- helper function that makes a lookup table from the output of a previous
operator. the lookup table loses information about the anonymous outputs
of the operator, so it only makes sense to use this in internal nodes
whose vectors will be consumed by other operators, but not on the
top level operator, which may return anonymous columns which we will need to
keep around -}
solve :: Config -> M.RelExpr -> Either String Env
solve config relexp = solve' config relexp >>= makeEnv

solve' :: Config -> M.RelExpr -> Either String [ (Vexp, Maybe Name, ColInfo) ]

{- Table is a Special case. It gets vexprs for all the output columns.

If the output columns are not aliased, we can always
use their original names as output names. (ie, there are no
anonymous expressions in the list)

note: not especially dealing with % names right now
todo: using the table schema we can resolve % names before
      they get to the final voodoo
-}
solve' config M.Table { M.tablename
                      , M.tablecolumns } =
  let r = do col <- tablecolumns -- list monad
             let colnam = fst col
             let qnam = makeqname tablename colnam
             let alias = Just $ getname col
             return $ do (_,info) <- getinfo qnam  -- either mnd
                         return $ (Load colnam, alias, info)
  in sequence r
  where getname (orig, Nothing) = orig
        getname (_, Just x) = x
        makeqname (Name tab) (Name col) = Name (tab ++ col)
        getinfo n = NameTable.lookup n (colinfo config)

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
solve' config M.Project { M.child, M.projectout, M.order = [] } =
  do env <- solve config child -- either monad
     let r = (do (expr, malias)  <- projectout -- list monad
                 let originalname = maybeGetName expr
                 let outputname = solveName (originalname, malias)
                 return $ do (vexpr,info) <- fromScalar env expr --either monad
                             return (vexpr, outputname, info))
     sequence r
  where maybeGetName (M.Ref orig) = Just orig
        maybeGetName _ = Nothing
        solveName (_, Just alias) = Just alias {-alias always wins-}
        solveName (Just some, Nothing) = Just some
        solveName _ = Nothing

{- the easy group by case: static single group -}
solve' config M.GroupBy { M.child,
                          M.inputkeys=[],
                          M.outputkeys=[],
                          M.outputaggs } =
  do env  <- solve config child --either monad
     sequence $ (do (agg, alias) <- outputaggs -- list monad
                    return ( do (sagg, agginfo)  <- solveAgg config env Nothing agg
                                -- either monad
                                return (sagg, alias, agginfo)))

{-group by one column-}
solve' config M.GroupBy { M.child,
                          M.inputkeys=[keyname], -- single input key for now.
                          M.outputkeys=[], -- deal with output keys later
                          M.outputaggs } =
  do  env  <- solve config child --either monad
      sequence $ (do (agg, alias) <- outputaggs -- list monad
                     return ( do (sagg, agginfo)  <- solveAgg config env (Just keyname) agg
                                 -- either monad
                                 return (sagg, alias, agginfo)))

solve' _ (M.GroupBy _ _ _ _) =
  Left $ "only able to compile aggregates with no data\
         \dependent grouping right now "

{-direct, foreign key join of two tables.
for now, this join assumes but does not check
that the two tables have a fk dependency between them

the reason only tables are allowed as right-children is
that after filtering the right child, the old position pointers
for the  are effectively invalid. On the other hand, the left table
can be anything, as the pointers are still valid.

(eg, after a filter on the right child...)
-}
-- solve' config M.FKJoin { M.table -- can be derived rel
--                , M.references = references@(M.Table _ _) -- only unfiltered rel.
--                , M.idxcol
--                } =
--   do Env leftcols leftenv  <- solve config table
--      Env rightcols  _ <- solve config references
--      let (rightvecs, ralias) = unzip rightcols
--      (_, shpos) <- NameTable.lookup idxcol leftenv
--      let joinCol shsource  = Shuffle {shop=Gather, shsource, shpos}
--      let gatheredcols = zip (map joinCol rightvecs) ralias
--      return $ leftcols ++ gatheredcols -- names are preserved.


-- solve' _ (M.FKJoin _ _ _) = Left $ "unsupported fkjoin (only fk-like\
-- \ joins with a plain table allowed)"


-- solve' config M.Select { M.child -- can be derived rel
--                , M.predicate
--                } =
--   do childenv@(Env childcols _) <- solve config child
--      let (childvecs, chalias) = unzip childcols
--      preds <- sc childenv predicate
--      -- use 0,1,2... for fold select groups, fully parallel
--      let idxs = Fold { foldop=FSel, fgroups=pos_ preds, fdata=preds }
--      let gatherQual col = Shuffle {shop=Gather, shsource=col, shpos=idxs}
--      let gatheredcols = zip (map gatherQual childvecs) chalias
--      return $ gatheredcols -- same names


solve' _ r_  = Left $ "unsupported M.rel:  " ++ groom r_

{- makes a vector from a scalar expression, given a context with existing
defintiions -}
fromScalar ::  Env -> M.ScalarExpr  -> Either String (Vexp, ColInfo)
fromScalar = sc

{- notes about tmp naming in Monet:
 -user columns are only named with lowercase.
 -temporary columns are sometimes named things like L.L or L1.L1.
 -to the right of 'as', we get fully qualified names
 -but sometimes, as a reference, they don't include the fully qualified names.
eg [L1 as L1]. This means we cannot just use a map in those cases, since
a search for L1 should potentially mean L1.L1.
-}
sc ::  Env -> M.ScalarExpr -> Either String (Vexp,ColInfo)
sc (Env _ env) (M.Ref refname)  =
  case (NameTable.lookup refname env) of
    Right (_, v) -> Right v
    Left s -> Left s


-- TODO: strictly speaking, I need to know the orignal type in order
-- to know What kind of computation is actually involved. Right now,
-- the original type is always assumed to be an integer...
-- Also, voodoo does not have support to express wider /narrower types.
-- For now, make casting data into a noop
sc env (M.Cast { M.mtype, M.arg }) =
  case mtype of
    M.MTinyint -> sc env arg
    M.MInt -> sc env arg
    M.MBigInt -> sc env arg
    M.MSmallint -> sc env arg
    M.MChar -> sc env arg -- assuming the input has already been converted
    M.MDouble -> sc env arg -- assume it was an integer originally...
    M.MDecimal _ dec ->
      do ch <- sc env arg -- multiply by 10^dec. hope there is no overflow.
         return  Binop { binop=M.Mul, left = ch, right = const_ (10 ^ dec) ch }
    othertype -> Left $ "unsupported type cast: " ++ groom othertype

sc env (M.Binop { M.binop, M.left, M.right }) =
  do l <- sc env left
     r <- sc env right
     return $ Binop { binop=binop, left=l, right=r }

sc env M.In { M.left, M.set } =
  do sleft <- sc env left
     sset <- mapM (sc env) set
     (f,rest) <- case map (==. sleft) sset of
                      [] -> Left "empty list"
                      a:b -> Right (a,b)
     return $ foldl' (||.) f rest

sc (Env lst _) (M.IntLiteral n) = return $ const_ n (fst $ head lst)

sc env (M.Unary { M.unop=M.Year, M.arg }) =
  --assuming input is well formed and the column is an integer representing
  --a day count from 0000-01-01)
  do dateval <- sc env arg
     return $ Binop { binop=Div, left=dateval, right=const_ 365 dateval}

sc env (M.IfThenElse { M.if_=mif_, M.then_=mthen_, M.else_=melse_ })=
  do if_ <- sc env mif_
     then_ <- sc env mthen_
     else_ <- sc env melse_
     return $ if_ ?. (then_,else_)

sc _ r = Left $ "(Vlite) unsupported M.scalar: " ++ (take 50  $ show  r)

solveAgg :: Config -> Env -> Maybe Name -> M.GroupAgg -> Either String (Vexp, ColInfo)

-- average case dealt with by rewriting
solveAgg config env mkey (M.Avg expr) =
  do sums <- solveAgg config env mkey (M.Sum expr)
     counts <- solveAgg config env mkey M.Count
     return $ Binop { binop=Div, left=sums, right=counts }

-- non avg. aggregates
solveAgg config env mkey agg =
  let Env ( (refv,_,_) : _ ) _ = env --get a ref vector for ranges
  in do fgroups <- (case mkey of -- maybe monad
                       Nothing -> Just $ zeros_ refv
                       Just key -> makeGroups key)
        (fdata, foldop) <- (case agg of
                               M.Sum expr ->
                                 do r <- sc env expr
                                    return $ (r, FSum)
                               M.Count -> Right $ (ones_ refv, FSum)
                               _ -> Left $ "unsupported agg")
        return $ Fold {foldop, fgroups, fdata}
  where makeGroups keyname =
          do (keyvec,info) <- NameTable.lookup keyname env
             let pivots = makeRange (lower info) (upper info)
             return $ Partition { pivots, pdata=keyvec }
        makeRange rmin max =
          RangeC { rmin
                 , rcount = ( max - rmin  + 1 )
                 , rstep = 1 }

-- make2LevelFold :: Config -> FoldOp -> Vexp -> (Vexp, ColInfo)
-- make2LevelFold config foldop fdata = -- adds one level of parallelism
--   let firstlevel =
--         Fold { foldop
--              , fgroups = groups_ (grainsizelg config) fdata -- parallel
--              , fdata }
--       secondlevel =
--         Fold { foldop
--              , fgroups = zeros_ firstlevel -- sequential
--              , fdata = firstlevel }
--   in  secondlevel
