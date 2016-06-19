module Vlite( vexpsFromMplan
            , Vexp(..)
            , Vx(..)
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
import qualified Error as E
import Error(Err)
import Error(check)
import Data.Bits
--import Debug.Trace
import Text.Groom
--import qualified Data.Map.Strict as Map
--import Data.String.Utils(join)
import Data.List.NonEmpty(NonEmpty(..))
--type Map = Map.Map
--type VexpTable = Map Vexp Vexp  --used to dedup Vexps
import Text.Printf
import Control.Exception.Base


type NameTable = NameTable.NameTable

data ShOp = Gather | Scatter
  deriving (Eq, Show, Generic,Ord)
instance NFData ShOp

data FoldOp = FSum | FMax | FMin | FSel
  deriving (Eq, Show, Generic, Ord)
instance NFData FoldOp

data Vx =
  Load Name
  | RangeV { rmin :: Integer, rstep :: Integer, rref::Vexp }
  | RangeC { rmin :: Integer, rstep :: Integer, rcount::Integer }
  | Binop { binop :: BinaryOp, left :: Vexp, right :: Vexp }
  | Shuffle { shop :: ShOp, shsource :: Vexp, shpos :: Vexp }
  | Fold { foldop :: FoldOp, fgroups :: Vexp, fdata :: Vexp }
  | Partition { pivots:: Vexp, pdata::Vexp }
  deriving (Eq,Show,Generic)
instance NFData Vx

data Vexp = Vexp Vx ColInfo (Maybe Name) deriving (Eq,Show,Generic)
instance NFData Vexp

-- instance Monad W where
--   W vx  >>= fn = fn vx
--   return a = W a
--- Note: if I want vectors to keep their names, then all of these functions
--- that make a new vector need to have names for their inner vectors.

{- some convenience vectors -}
pos_ :: Vexp -> Err Vexp
pos_ v = complete $ RangeV {rmin=0, rstep=1, rref=v}

const_ :: Integer -> Vexp -> Err Vexp
const_ k v = complete $ RangeV{rmin=k, rstep=0, rref=v}

zeros_ :: Vexp -> Err Vexp
zeros_ = const_ 0

ones_ :: Vexp -> Err Vexp
ones_ = const_ 1

(==.) :: Vexp -> Vexp -> Err Vexp
a ==. b = makeBinop Eq a b

(>>.) :: Vexp -> Vexp -> Err Vexp
a >>. b = makeBinop BitShift a b

(<<.) :: Vexp -> Vexp -> Err Vexp -- BitShift uses sign to encode direction
a <<. b = do z <- zeros_ b
             negb <- (z -. b)
             a >>. negb

(||.) :: Vexp -> Vexp -> Err Vexp
a ||. b = makeBinop LogOr a b

(|.) :: Vexp -> Vexp -> Err Vexp
a |. b = makeBinop BitOr a b

(&.) :: Vexp -> Vexp -> Err Vexp
a &. b = makeBinop BitAnd a b

(-.) :: Vexp -> Vexp -> Err Vexp
a -. b = makeBinop Sub a b

(*.) :: Vexp -> Vexp -> Err Vexp
a *. b = makeBinop Mul a b

(+.) :: Vexp -> Vexp -> Err Vexp
a +. b = makeBinop Add a b

(/.) :: Vexp -> Vexp -> Err Vexp
a /. b = makeBinop Div a b

makeBinop :: BinaryOp -> Vexp -> Vexp -> Err Vexp
makeBinop binop left right = complete $ Binop {binop, left, right}

(?.) :: Vexp -> (Vexp,Vexp) -> Err Vexp
cond ?. (a,b) =   do ones <- ones_ cond
                     zeros <- zeros_ cond
                     negcond <- cond ==. zeros
                     -- need to make condition boolean for mult.
                     poscond <- ones -. negcond
                     left <- poscond *. a
                     right <- negcond *. b
                     left +. right

gather :: Vexp -> Vexp -> Vexp
gather dat@(Vexp _ (ColInfo {bounds}) _)  pos@(Vexp _ (ColInfo {count}) _) =
  let vx = Shuffle {shop=Gather, shsource=dat, shpos=pos}
      info = ColInfo { bounds, count }
      -- data vals bounded by data values.
      -- size bounded by count array.
  in (Vexp vx info Nothing)

complete :: Vx -> Either String Vexp
complete vx =
  do colinfo <- inferMetadata vx
     return (Vexp vx colinfo Nothing)

inferMetadata :: Vx -> Either String ColInfo

inferMetadata RangeV {rmin=rstart,rstep,rref=(Vexp _ (ColInfo {count}) _)}
  =  let extremes = [rstart, rstart + count*rstep]
     in return $ ColInfo { bounds=(minimum extremes, maximum extremes)
                         , count }

inferMetadata RangeC {rmin=rstart, rstep, rcount}
  =  let extremes = [rstart + rcount*rstep, rstart]
     in return $ ColInfo { bounds=(minimum extremes, maximum extremes)
                         , count=rcount }

inferMetadata Shuffle { shop=_
                      , shsource=(Vexp _ (ColInfo {bounds=sourcebounds})  _)
                      , shpos=(Vexp _ (ColInfo {bounds=(_,posmax)}) _) }
  = return $ ColInfo { bounds=sourcebounds, count=posmax }


inferMetadata Fold { foldop=FSel
                   , fgroups=_
                   , fdata=(Vexp _ (ColInfo {count}) _) } =
  return $ ColInfo {bounds=(0, count-1), count} -- coefficients

inferMetadata Fold { foldop
                      , fgroups = (Vexp _ (ColInfo {count=gcount}) _)
                      , fdata = (Vexp _ (ColInfo {bounds=(dlower,dupper), count=dcount}) _)
                      } =
  do check (gcount,dcount) (\(a,b) -> a == b) "suspicious: group and data count bounds dont match"
     case foldop of
       FSum -> let extremes = [dlower, dlower*dcount, dupper, dupper*dcount]
                   -- for positive dlower, dlower is the minimum.
                   -- for negative dlower, dlower*dcount is the minimum, and so on.
               in return $ ColInfo (minimum extremes, maximum extremes) dcount
       FMax -> return $ ColInfo (dlower, dupper) dcount
       FMin -> return $ ColInfo (dlower, dupper) dcount
       FSel -> Left "use different handler for select"

-- the result of partition is a list of indices that, for each pdata
-- tells it where the first element in pivots is that is larger or equal to it.
-- so, the outputsize is the same as that of pdata
-- and, if there is a bound for each element in the input,
-- the output values are in the domain [0..(count pivots  - 1)]
inferMetadata Partition
  { pivots=(Vexp _ (ColInfo {count=pivotcount}) _)
  , pdata=(Vexp _ (ColInfo {count=datacount}) _)
  } = return $ ColInfo {bounds=(0,pivotcount-1), count=datacount}


inferMetadata Binop
  { binop
  , left=left@(Vexp _ (ColInfo {bounds=(l1,u1), count=c1 }) _)
  , right=right@(Vexp _ (ColInfo {bounds=(l2,u2), count=c2 }) _)
  } = do
         let count = min c1 c2
         bounds <- case binop of
            Gt ->  Right (0,1)
            Lt ->  Right (0,1)
            Eq ->  Right (0,1)
            Neq ->  Right (0,1)
            Geq ->  Right (0,1)
            Leq ->  Right (0,1)
            LogAnd ->  Right (0,1)
            LogOr ->  Right (0,1)
            Add ->  Right (l1 + l2, u1 + u2)
            Sub ->  Right (l1 - u2, u1 - l2) -- notice swap
            Mul -> let allpairs = sequence [[l1,u1],[l2,u2]] -- cross product
                       prods = map (foldl (*) 1) allpairs
                   in  Right (minimum prods, maximum prods) -- TODO double check reasoning here.
            Div -> let allpairs = [(l1,l2), (l1,u2), (u1,l2), (u1,u2)]
                       divs = map (\(x,y) -> x `div` y) allpairs
                   in  Right (minimum divs, maximum divs) -- TODO double check reasoning here.
            Min -> Right (min l1 l2, min u1 u2) -- this is true, right?
            Max -> Right (max l1 l2, max u1 u2)
            Mod -> Right (0, u2) -- assuming mods are always positive
            BitAnd -> if (l1 >= 0 && l2 >= 0 && u1 < (1 `shiftL` 31) && u2 < (1 `shiftL` 31))
                      then do mx <- min (maxForWidth left) (maxForWidth right)
                              return (0,mx) -- precision could be improved.
                      else Left $ E.todo "cant deduce BitAnd bounds" ((l1,u1),(l2,u2))
            BitOr -> if (l1 >= 0 && l2 >= 0 && u1 < (1 `shiftL` 31) && u2 < (1 `shiftL` 31))
                     then do mx <- max (maxForWidth left) (maxForWidth right)
                             return (0,mx) -- precision could be improved.
                     else Left $ E.todo "cant deduce BitOr bounds" ((l1,u1),(l2,u2))
            BitShift -> do check u2 (< 32) "shift left by more than 31?"
                           check l2 (> -32) "shift right by more than 31?"
                           -- shift left is like multiplication (amplifies neg and pos)
                           -- shift right is like division (shrinks numbers neg and pos)
                           -- both can happen in a single call to shift...
                           let mshift (a,b) = let shfmask = (fromInteger $ toInteger $ abs b)
                                              in if b < 0 then (a `shiftR` shfmask)
                                                 else a `shiftL` shfmask
                           let allpairs = [(l1,l2), (l1,u2), (u1,l2), (u1,u2)]
                           let extremes = map mshift allpairs
                           return $ (minimum extremes, maximum extremes)
         return $ ColInfo {bounds, count}

inferMetadata vx = Left $ E.todo  "info deduction" vx

vexpsFromMplan :: M.RelExpr -> Config -> Either String [Vexp]
vexpsFromMplan r c  = solve' c r

outputName :: (M.ScalarExpr, Maybe Name) -> Maybe Name
outputName (_, Just alias) = Just alias {-alias always wins-}
outputName ((M.Ref orig), Nothing) = Just orig {-anon still has name if ref-}
outputName _ = Nothing

-- includes both the list of all entries, as well
-- as a lookup friendly map
data Env = Env [Vexp] (NameTable Vexp)

makeEnv :: [Vexp] -> Either String Env
makeEnv lst =
  do tbl <- makeTable lst
     return $ Env lst tbl
  where makeTable pairs = foldM maybeadd NameTable.empty pairs
        maybeadd env (Vexp _ _ Nothing) = Right env
        maybeadd env vexp@(Vexp _ _ (Just newalias)) =
          NameTable.insert newalias vexp env


makeEnvWeak :: [Vexp] -> Env
makeEnvWeak lst =
  let tbl = makeTable lst
  in Env lst tbl
  where makeTable pairs = foldl' maybeadd NameTable.empty pairs
        maybeadd env (Vexp _ _ Nothing) = env
        maybeadd env vexp@(Vexp _ _ (Just newalias)) =
          NameTable.insertWeak newalias vexp env

{- helper function that makes a lookup table from the output of a previous
operator. the lookup table loses information about the anonymous outputs
of the operator, so it only makes sense to use this in internal nodes
whose vectors will be consumed by other operators, but not on the
top level operator, which may return anonymous columns which we will need to
keep around -}
solve :: Config -> M.RelExpr -> Either String Env
solve config relexp = solve' config relexp >>= makeEnv

solve' :: Config -> M.RelExpr -> Either String [Vexp]

{- Table is a Special case. It gets vexprs for all the output columns.

If the output columns are not aliased, we can always
use their original names as output names. (ie, there are no
anonymous expressions in the list)

note: not especially dealing with % names right now
todo: using the table schema we can resolve % names before
      they get to the final voodoo
-}
solve' config M.Table { M.tablename=_
                      , M.tablecolumns } =
  let tidcols = filter isTid tablecolumns
      nontid = filter (not . isTid) tablecolumns
      nontidloads =
        sequence $ (do col@(colnam, _) <- nontid -- list monad
                       let alias = Just $ decideAlias col
                       return $ (do (_,info) <- getinfo colnam  -- either monad
                                    return $ Vexp (Load colnam) info alias))
  in do snontids@(refv@(Vexp _ _ _ ) : _)  <- nontidloads --either
        case tidcols of
          [] -> return $ snontids
          [tidcol] -> do (Vexp vx clinfo _)  <- pos_ refv
                         let named = Vexp vx clinfo $ Just $ decideAlias tidcol
                         return $ named : snontids
          _ -> Left $ E.unexpected "multiple tidcols defined in table" tidcols
  where decideAlias (orig, Nothing) = orig
        decideAlias (_, Just x) = x
        isTid (Name [_, "%TID%"], _) = True
        isTid (Name [_, _], _) = False
        isTid _ = undefined -- this should never happen. used here for warning.
        getinfo n = NameTable.lookup n (colinfo config)

        ---not expecting names with more than two components

{- Project: not dealing with ordered queries right now
note. project affects the name scope in the following ways

There are four cases illustrated below:
project (...) [ foo, bar as bar2, sum(bar, baz) as sumbaz, sum(bar, bar) ]

For the consumer to be able to see all relevant columns, we need
to solve the cases like this:

Note: a name is within scope starting from its position in the output list,
ie, later expressions in the name list can refer to previous expressions
in the name list.  this isn't common ( but it happens in tpch plans )

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
  do (Env list0 _) <- solve config child -- either monad -- used for reading
     (_ , solved) <- foldM foldFun (list0,[]) projectout
     return $ solved
     where addEntry (list0, acclist) tup = (list0, tup : acclist) -- adds output to acclist
           foldFun lsts@(list0, acclist) arg@(expr, _) =
             do asenv <- return $ makeEnvWeak $ list0 ++ acclist-- use both lists for name resolution
                 -- allow collisions b/c they do happen .
                (Vexp vx info _)  <- fromScalar asenv expr --either monad
                return $ addEntry lsts $ Vexp vx  info $ outputName arg
           -- solveForward nt (M.Ref col, malias) = do (_, (x,y) ) <- NameTable.lookup col nt
           --                                          return (case malias of
           --                                                     Nothing -> (x, Just col, y)
           --                                                     Just alias -> (x, Just alias, y))
           -- solveForward _ _ = Left $ "this input is not a forward"


-- problem:
-- n2.n_name as all_nations.nation defined then directly uesd in a subsequent expression. not found.
-- L2 as L2.L2 used to redefined L2.L2 from a sub-output (name clash on L2.L2) then not used
-- in the  same project list .


solve' config M.GroupBy { M.child,
                          M.inputkeys,
                          M.outputaggs } =
  do env@(Env (refv:_) nt)  <- solve config child --either monad
     lookedup  <- (mapM (\n -> (NameTable.lookup n nt)) inputkeys)
     let keyvecs = map snd lookedup
     gbkeys <- case keyvecs of
       [] -> zeros_ refv >>=  (return . (:| []))
       a : rest -> return $ a :| rest
     gkey@(Vexp _ (ColInfo {bounds=(gmin, _) }) _) <- makeCompositeKey gbkeys
     check gmin (== 0) "for a group key expecting gmin to be 0"
     sequence $ (do pr@(agg, _) <- outputaggs -- list monad
                    return ( do (Vexp sagg agginfo _)  <- solveAgg config env gkey agg
                                let outalias = case pr of
                                      (M.GDominated n, Nothing) -> Just n
                                      (_, alias) -> alias
                                      _ -> Nothing
                                return (Vexp sagg agginfo outalias)))

{-direct, foreign key join of two tables.
for now, this join assumes but does not check
that the two tables have a fk dependency between them

the reason only tables are allowed as right-children is
that after filtering the right child, the old position pointers
for the  are effectively invalid. On the other hand, the left table
can be anything, as the pointers are still valid.

(eg, after a filter on the right child...)
-}
solve' config M.FKJoin { M.table -- can be derived rel
               , M.references = references@(M.Table _ _) -- only unfiltered rel.
               , M.idxcol
               } =
  do Env leftcols leftenv  <- solve config table
     Env rightcols  _ <- solve config references
     (_, fkcol) <- NameTable.lookup idxcol leftenv
     let sright = (do dimcol@(Vexp _ _ ralias) <- rightcols -- list monad
                      let (Vexp vx info _) = gather dimcol fkcol
                      return (Vexp vx info ralias))
     return $ leftcols ++ sright

solve' _ (M.FKJoin _ _ _) = Left $ "unsupported fkjoin (only fk-like\
\ joins with a plain table allowed)"

solve' config M.Select { M.child -- can be derived rel
                       , M.predicate
                       } =
  do childenv@(Env childcols _) <- solve config child -- either monad
     fdata  <- sc childenv predicate
     fgroups <- pos_ fdata
     idx <- complete $ Fold {foldop=FSel, fgroups, fdata}
     return (do dat  <- childcols -- list monad
                return $ gather dat idx)

solve' _ r_  = Left $ "unsupported M.rel:  " ++ groom r_

{- makes a vector from a scalar expression, given a context with existing
defintiions -}
fromScalar ::  Env -> M.ScalarExpr  -> Either String Vexp
fromScalar = sc

{- notes about tmp naming in Monet:
 -user columns are only named with lowercase.
 -temporary columns are sometimes named things like L.L or L1.L1.
 -to the right of 'as', we get fully qualified names
 -but sometimes, as a reference, they don't include the fully qualified names.
eg [L1 as L1]. This means we cannot just use a map in those cases, since
a search for L1 should potentially mean L1.L1.
-}
-- sc' :: Env -> M.ScalarExpr -> Either String Vexp
-- sc' env expr =
--   do vx <- sc env expr
--      return $ getInfo vx

sc ::  Env -> M.ScalarExpr -> Either String Vexp
sc (Env _ env) (M.Ref refname)  =
  case (NameTable.lookup refname env) of
    Right (_, v) -> Right v
    Left s -> Left s

-- serious TODO: strictly speaking, I need to know the orignal type in order
-- to correctly produce code here.
-- We need the input type bc, for example Decimal(10,2) -> Decimal(10,3) = *10
-- but Decimal(10,1) -> Decimal(10,3) = * 100. Right now, that input type is not explicitly given.
sc env (M.Cast { M.mtype, M.arg }) =
  case mtype of
    M.MTinyint -> sc env arg
    M.MInt -> sc env arg
    M.MBigInt -> sc env arg
    M.MSmallint -> sc env arg
    M.MChar -> sc env arg -- assuming the input has already been converted
    M.MDouble -> sc env arg -- assume it was an integer originally...
    M.MDecimal _ dec -> --we are assuming inputs are integers, but should not.
      do orig <- sc env arg
         -- multiply by 10^dec. hope there is no overflow.
         -- TODO now we could statically check for overflow using sizes
         let factor = (10 ^ dec)
         fvec <- const_ factor orig
         orig *. fvec
    othertype -> Left $ "unsupported type cast: " ++ groom othertype


sc env (M.Binop { M.binop, M.left, M.right }) =
  do l <- sc env left
     r <- sc env right
     complete $ Binop {binop, left=l, right=r}

sc env M.In { M.left, M.set } =
  do sleft  <- sc env left
     sset <- mapM (sc env) set
     eqs <- mapM (==. sleft) sset
     (first,rest) <- case eqs of
       [] -> Left "empty list"
       a:b -> Right (a,b)
     foldM (||.) first rest

sc (Env (vref : _ ) _) (M.IntLiteral n)
  = const_ n vref

sc env (M.Unary { M.unop=M.Year, M.arg }) =
  --assuming input is well formed and the column is an integer representing
  --a day count from 0000-01-01)
  do dateval <- sc env arg
     v365 <- const_ 365 dateval
     dateval /. v365

--example use of isnull. In all the contexts of TPCH queries i saw, the isnull is called on
--a column or derived column that is statically known to not be null, so we just remove that.
{- sys.ifthenelse(sys.isnull(sys.=(all_nations.nation NOT NULL, char(25)[char(6) "BRAZIL"])), boolean "false", sys.=(all_nations.nation NOT NULL, char(25)[char(6) "BRAZIL"])) -}
sc env (M.IfThenElse { M.if_=M.Unary { M.unop=M.IsNull
                                     , M.arg=oper1 } , M.then_=M.IntLiteral 0, M.else_=oper2 } )=
  do check (oper1,oper2) (\(a,b) -> a == b) "different use of isnull than expected"
     sc env oper1 --just return the guarded operator.

-- note: for max and min, the actual possible bounds are more restrictive.
-- for now, I don't care.
sc env (M.IfThenElse { M.if_=mif_, M.then_=mthen_, M.else_=melse_ })=
  do if_ <- sc env mif_
     then_ <- sc env mthen_
     else_ <- sc env melse_
     if_ ?.(then_,else_)

sc _ r = Left $ "(Vlite) unsupported M.scalar: " ++ (take 50  $ show  r)

solveAgg :: Config -> Env -> Vexp -> M.GroupAgg -> Either String Vexp

solveAgg _ (Env [] _) _ _  = Left $"empty input for group by"

-- average case dealt with by rewriting tp sum, count and finally using division.
solveAgg config env gkeyvec (M.GAvg expr) =
  do gsums@(Vexp _ (ColInfo { count=cgsums }) _) <- solveAgg config env gkeyvec (M.GFold M.FSum expr)
     gcounts@(Vexp _ (ColInfo { bounds=(minc,_ ), count=cgcounts}) _) <- solveAgg config env gkeyvec M.GCount
     check (cgsums,cgcounts) (\(a,b) -> a == b) "counts should match"
     check minc (== 1) $ "minimum count is always 1 but here it is " ++ show minc
     gsums /. gcounts

-- all keys in a column dominated by groups are equal to their max
-- so deal with this as if it were a max
solveAgg config env gkeyvec (M.GDominated nm) =
  solveAgg config env gkeyvec (M.GFold M.FMax (M.Ref nm))

-- count is rewritten to summing a one for every row
solveAgg config env gkeyvec (M.GCount) =
  let rewrite = (M.GFold M.FSum (M.IntLiteral 1))
  in solveAgg config env gkeyvec rewrite

solveAgg config env gkeyvec (M.GFold op expr) =
  do scattermask <- getScatterMask gkeyvec
     sortedGroups <- complete $ Shuffle {shop=Scatter, shpos=scattermask, shsource=gkeyvec}
     gdata <- sc env expr
     sortedData <- complete $ Shuffle {shop=Scatter, shpos=scattermask, shsource=gdata}
     let foldop = case op of
           M.FSum -> FSum
           M.FMax -> FMax
     make2LevelFold config foldop sortedGroups sortedData

-- scatter mask for a group by
getScatterMask :: Vexp -> Either String Vexp
getScatterMask pdata@(Vexp _ (ColInfo {bounds=(pdatamin, pdatamax)}) _) =
  do pivots <- complete $ RangeC { rmin=pdatamin, rstep=1, rcount=pdatamax }
     complete $ Partition { pivots, pdata }

maxForWidth :: Vexp -> Err Integer
maxForWidth vec =
  do let width = toInteger (getBitWidth vec)
  -- examples:
  -- bitwidth is 0, then max should be 0:  (1 << 0) - 1 = (1 - 1) = 0
  -- bitwidth is 1, then max should be 0b1. (1 << 1) - 1 = (2 - 1) = 1 = 0xb1
  -- bitwidth is 2, then max should be 0xb11. (1 << 2) -1 = (4 - 1) = 3 = 0xb11
  -- cannot really subtract 1 from 1 << 31, b/c is underflow. so just check.
     check width (< 31) "about to shift by more than 31"
     return $ (1 `shiftL` (fromInteger width)) - 1

makeCompositeKey :: NonEmpty Vexp -> Either String Vexp
makeCompositeKey (firstvexp :| rest) =
  do out <- foldM composeKeys firstvexp rest
     maxval <- maxForWidth out
     maxvalV <- const_ maxval out
     out &. maxvalV  --mask used as a hint to Voodoo (to infer size)

--- makes the vector min be at 0 if it isnt yet.
shiftToZero :: Vexp -> Err Vexp
shiftToZero arg@(Vexp _ (ColInfo {bounds=(vmin,_)}) _)
  = if vmin == 0 then return arg
    else do vminv <- const_ vmin arg
            arg -. vminv

-- bitwidth required to represent all members
getBitWidth :: Vexp -> Integer
getBitWidth (Vexp _ (ColInfo {bounds=(l,u)}) _)
  = fromInteger $ max (bitsize l) (bitsize u)

bitsize :: Integer -> Integer
bitsize num =
  if num >= 0 then
    if num < toInteger (maxBound :: Int32) then
      let num32 = (fromInteger num) :: Int32
          ans = toInteger $ (finiteBitSize num32) - (countLeadingZeros num32)
      in assert (ans <= 31) ans
    else error (printf "number %d is larger than maxInt32" num)
  else error (printf "bitwidth only allowed for non-negative numbers (num=%d)" num)

composeKeys :: Vexp -> Vexp -> Either String Vexp
composeKeys l r =
  do sleft <- shiftToZero l
     sright <- shiftToZero r
     let oldbits = getBitWidth sleft
     let deltabits = getBitWidth sright
     let newbits = oldbits + deltabits
     check newbits (<= 32) "cannot compose keys to something larger than 32 bits"
     check deltabits (< 32) "we are shifting a 32 bit int by >=32 bits. undefined in C."
     dbits <- const_  deltabits sright
     shiftedleft <- sleft <<. dbits -- make space
     shiftedleft |. sright

-- Assumes the fgroups are alrady sorted
make2LevelFold :: Config -> FoldOp -> Vexp -> Vexp -> Either String Vexp
make2LevelFold config foldop fgroups fdata =
  do pos <- pos_ fgroups
     gsize <- const_ (grainsizelg config) fgroups
     level1par <- pos >>. gsize
     level1groups <- composeKeys fgroups level1par
     level1results <- complete $ Fold { foldop, fgroups=level1groups, fdata }
     level2results <- complete $ Fold { foldop, fgroups, fdata=level1results }
     return level2results
