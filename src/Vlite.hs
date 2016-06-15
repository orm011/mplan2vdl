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
import qualified Error as E
import Error(check)
import Data.Bits
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

data Vexp  =
  Load Name
  | RangeV { rmin :: Int64, rstep :: Int64, rref::Vexp }
  | RangeC { rmin :: Int64, rstep :: Int64, rcount::Int64 }
  | Binop { binop :: BinaryOp, left :: Vexp, right :: Vexp }
  | Shuffle { shop :: ShOp, shsource :: Vexp, shpos :: Vexp }
  | Fold { foldop :: FoldOp, fgroups :: Vexp, fdata :: Vexp }
  | Partition { pivots::Vexp, pdata::Vexp }
  deriving (Eq,Show,Generic,Ord)
instance NFData Vexp

{- some convenience vectors -}
const_ :: Int64 -> Vexp -> Vexp
const_ k v = RangeV{ rmin = k, rstep = 0, rref = v }

pos_ :: Vexp -> Vexp
pos_ v = RangeV{ rmin = 0, rstep = 1, rref = v }

ones_ :: Vexp -> Vexp
ones_ = const_ 1

zeros_ :: Vexp -> Vexp
zeros_ = const_ 0


(==.) :: Vexp -> Vexp -> Vexp
a ==. b = Binop { binop=Eq, left=a, right = b}

(>>.) :: Vexp -> Vexp -> Vexp
a >>. b = Binop { binop=BitShift, left=a, right = b}

(<<.) :: Vexp -> Vexp -> Vexp -- BitShift uses sign to encode direction
a <<. b = a >>. (zeros_ b -. b)

(||.) :: Vexp -> Vexp -> Vexp
a ||. b = Binop { binop=LogOr, left=a, right=b}

(|.) :: Vexp -> Vexp -> Vexp
a |. b = Binop { binop=BitOr, left=a, right=b}

(&.) :: Vexp -> Vexp -> Vexp
a &. b = Binop { binop=BitAnd, left=a, right=b}

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

outputName :: (M.ScalarExpr, Maybe Name) -> Maybe Name
outputName (_, Just alias) = Just alias {-alias always wins-}
outputName ((M.Ref orig), Nothing) = Just orig {-anon still has name if ref-}
outputName _ = Nothing

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

makeEnvWeak :: [(Vexp, Maybe Name, ColInfo)] -> Env
makeEnvWeak lst =
  let tbl = makeTable lst
  in Env lst tbl
  where makeTable pairs = foldl' maybeadd NameTable.empty pairs
        maybeadd env (_, Nothing, _) = env
        maybeadd env (vexp, Just newalias, sz) =
          NameTable.insertWeak newalias (vexp,sz) env


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
solve' config M.Table { M.tablename=_
                      , M.tablecolumns } =
  let tidcols = filter isTid tablecolumns
      nontid = filter (not . isTid) tablecolumns
      r = (do col@(colnam, _) <- nontid -- list monad
              let alias = Just $ decideAlias col
              return $ do (_,info) <- getinfo colnam  -- either monad
                          return $ (Load colnam, alias, info))
      in do snontid@( (refv,_,ColInfo {count} ) : _ ) <- sequence r --either
            case tidcols of
              [] -> return $ snontid
              [tidcol] -> let stidcol  = ( pos_ refv  -- manufactured
                                         , Just $ decideAlias tidcol
                                         , ColInfo {bounds=(0,count-1), count})
                              in return $ stidcol : snontid
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

Note: a name is within scope starting from its position in the output list.

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
                (vexpr,info) <- fromScalar asenv expr --either monad
                return $ addEntry lsts (vexpr, outputName arg, info)
           -- solveForward nt (M.Ref col, malias) = do (_, (x,y) ) <- NameTable.lookup col nt
           --                                          return (case malias of
           --                                                     Nothing -> (x, Just col, y)
           --                                                     Just alias -> (x, Just alias, y))
           -- solveForward _ _ = Left $ "this input is not a forward"


-- problem:
-- n2.n_name as all_nations.nation defined then directly uesd in a subsequent expression. not found.
-- L2 as L2.L2 used to redefined L2.L2 from a sub-output (name clash on L2.L2) then not used
-- in the  same project list, but using naive a


solve' config M.GroupBy { M.child,
                          M.inputkeys,
                          M.outputaggs } =
  do env  <- solve config child --either monad
     sequence $ (do (agg, alias) <- outputaggs -- list monad
                    return ( do (sagg, agginfo)  <- solveAgg config env inputkeys agg
                                -- either monad
                                return (sagg, alias, agginfo)))

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
     (_, (fkcol, ColInfo { count=leftcount } )) <- NameTable.lookup idxcol leftenv
     let sright = (do (dimcolumn, ralias, ColInfo {bounds=rightbounds} ) <- rightcols -- list monad
                      let joinedrcol  = Shuffle {shop=Gather, shsource=dimcolumn, shpos=fkcol}
                      let colinfo = ColInfo { bounds=rightbounds, count=leftcount } -- bounds inherited from right, count from left.
                      return (joinedrcol, ralias, colinfo))
     return $ leftcols ++ sright

solve' _ (M.FKJoin _ _ _) = Left $ "unsupported fkjoin (only fk-like\
\ joins with a plain table allowed)"

solve' config M.Select { M.child -- can be derived rel
                       , M.predicate
                       } =
  do childenv@(Env childcols _) <- solve config child -- either monad
     (spred, _)  <- sc childenv predicate
     return (do (childvec, chalias, ColInfo {bounds=(l,u), count}) <- childcols -- list monad
                let idx = Fold { foldop=FSel, fgroups=pos_ spred, fdata=spred } -- fully parallel select.
                let gathered =  Shuffle {shop=Gather, shsource=childvec, shpos=idx }
                let ginfo = ColInfo { bounds=(l,u), count} -- in this case the count is the same as before.
                return (gathered, chalias, ginfo))

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
      do ( ch, ColInfo {bounds=(l,u), count} ) <- sc env arg -- multiply by 10^dec. hope there is no overflow.
         let factor = (10 ^ dec)
         return  ( Binop { binop=M.Mul, left = ch, right = const_  factor ch }
                 , ColInfo {bounds=(factor * l, factor * u ), count} )
    othertype -> Left $ "unsupported type cast: " ++ groom othertype


sc env (M.Binop { M.binop, M.left, M.right }) =
  do (l, ColInfo {bounds=(l1, u1), count=cl}) <- sc env left
     (r, ColInfo {bounds=(l2, u2), count=cr}) <- sc env right
     bounds <- (case binop of
                   M.Gt -> Right (0,1)
                   M.Lt -> Right (0,1)
                   M.Eq -> Right (0,1)
                   M.Neq -> Right (0,1)
                   M.Geq -> Right (0,1)
                   M.Leq -> Right (0,1)
                   M.LogAnd -> Right (0,1)
                   M.LogOr -> Right (0,1)
                   M.Add -> Right (l1 + l2, u1 + u2)
                   M.Sub -> Right (l1 - u2, u1 - l2) -- notice swap
                   M.Mul -> let allpairs = sequence [[l1,u1],[l2,u2]] -- cross product
                                prods = map (foldl (*) 1) allpairs
                            in Right (minimum prods, maximum prods) -- TODO double check reasoning here.
                   M.Div -> let allpairs = [(l1,l2), (l1,u2), (u1,l2), (u1,u2)]
                                divs = map (\(x,y) -> x `div` y) allpairs
                            in Right (minimum divs, maximum divs) -- TODO double check reasoning here.
                   M.Min -> Right (min l1 l2, min u1 u2) -- this is true, right?
                   M.Max -> Right (max l1 l2, max u1 u2)
                   ow_ -> Left $ E.todo "bounds for other operators" ow_
               )
     return $ ( Binop { binop, left=l, right=r }
              , ColInfo {bounds, count=(min cl cr)} )

sc env M.In { M.left, M.set } =
  do (sleft, ColInfo {count})  <- sc env left
     sset <- mapM (sc env) set
     (f,rest) <- case map (==. sleft) (map fst sset) of
                      [] -> Left "empty list"
                      a:b -> Right (a,b)
     return $ ( foldl' (||.) f rest
              , ColInfo {bounds=(0,1), count}) -- its boolean at the end

sc (Env ((vref, _, ColInfo {count}) : _ ) _) (M.IntLiteral n)
  = return $ (const_ n vref, ColInfo {bounds=(n,n), count})

sc env (M.Unary { M.unop=M.Year, M.arg }) =
  --assuming input is well formed and the column is an integer representing
  --a day count from 0000-01-01)
  do (dateval, ColInfo {bounds=(l,u), count}) <- sc env arg
     return $ ( Binop { binop=Div, left=dateval, right=const_ 365 dateval}
              , ColInfo {bounds=(l `div` 365, u `div` 365), count} )

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
  do (if_,_) <- sc env mif_
     (then_, ColInfo {bounds=(tl,tu), count}) <- sc env mthen_
     (else_, ColInfo {bounds=(el,eu)}) <- sc env melse_
     return $ (if_ ?.(then_,else_), ColInfo {bounds=(min tl el, max tu eu), count})

sc _ r = Left $ "(Vlite) unsupported M.scalar: " ++ (take 50  $ show  r)

solveAgg :: Config -> Env -> [Name] -> M.GroupAgg -> Either String (Vexp, ColInfo)

solveAgg _ (Env [] _) _ _  = Left $"empty input for group by"

-- average case dealt with by rewriting tp sum, count and finally using division.
solveAgg config env mkey (M.GAvg expr) =
  do (sums, ColInfo {bounds=(suml,sumu), count=countsums}) <- solveAgg config env mkey (M.GFold M.FSum expr)
     (counts, ColInfo {bounds=(cl,_), count=countc}) <- solveAgg config env mkey M.GCount
     check (countsums,countc) (\(a,b) -> a == b) "counts should match"
     check cl (== 1) $ "minimum count is always 1 but here it is " ++ show cl
     return $ ( Binop { binop=Div, left=sums, right=counts }
              , ColInfo {bounds=(suml,sumu), count=countc } ) -- bc counts can be 1

-- all keys in a column dominated by groups are equal to their max
-- so deal with this as if it were a max
solveAgg config env mkey (M.GDominated nm) =
  solveAgg config env mkey (M.GFold M.FMax (M.Ref nm))

-- count is rewritten to summing a one for every row
solveAgg config env mkey (M.GCount) =
  let rewrite = (M.GFold M.FSum (M.IntLiteral 1))
  in solveAgg config env mkey rewrite

solveAgg config env@(Env ((refv,_,refvInfo):_) nt) keynames (M.GFold op expr) =
  let mkeyvecs = do keyname <- keynames -- list
                    return $ (do (_, rvec) <- NameTable.lookup keyname nt -- either
                                 return $ rvec)
  in do keys <- sequence $ mkeyvecs -- either
        kinfo@(gkey, info@ColInfo {bounds=(gmin, _) }) <- makeCompositeKey (refv,refvInfo) keys
        check gmin (== 0) "for a group key expecting gmin to be 0"
        let scattermask = getScatterMask kinfo
        let sortedGroups = Shuffle {shop=Scatter, shpos=scattermask, shsource=gkey}
        let sortedGroupInfo = info -- this shuffle is a permuatation, so changes nothing.
        let sortedEnv  = shuffleAll env scattermask
        solveSortedAgg config sortedEnv (sortedGroups,sortedGroupInfo) op expr


solveSortedAgg :: Config -> Env -> (Vexp,ColInfo) -> M.FoldOp -> M.ScalarExpr -> Either String (Vexp,ColInfo)
solveSortedAgg config sortedEnv sortedGroups op expr =
  do (sortedData, ColInfo (lower,upper) cnt) <- sc sortedEnv expr
     let (foldop,clout) = case op of
           M.FSum -> (FSum, ColInfo (lower, cnt*upper) cnt)
           -- one group may consist of only the max, whereas another group may consist of all elts being upper (if lower == upper and there is one group)
           M.FMax -> (FMax, ColInfo (lower, upper) cnt)
     vout <- make2LevelFold config foldop sortedGroups sortedData
     return (vout, clout)

getScatterMask :: (Vexp, ColInfo) -> Vexp
getScatterMask (col, ColInfo {bounds=(colmin, colmax)}) =
  let pivots = RangeC { rmin=colmin , rcount=colmax+1, rstep = 1 }
  in Partition { pivots, pdata=col }


shuffleAll :: Env -> Vexp -> Env
shuffleAll (Env lst _) scattermask =
  let shlst = (do (vec, malias, colinfo) <- lst -- list monad
                 --min and max are the same. count may be different, unless it is a permuation.
                  return $ (Shuffle { shop=Scatter, shpos = scattermask, shsource=vec }, malias, colinfo))
  in makeEnvWeak shlst


bitsize :: (Int64, Int64) -> Int64
bitsize (minv,maxv) = let r = (maxv - minv)
                          asint =  (finiteBitSize r) - (countLeadingZeros r)
                          in fromInteger $ toInteger asint

normalize :: (Vexp, ColInfo) -> (Vexp, Int64)
normalize (expr, ColInfo { bounds=bounds@(minval,_) }) = (expr -. (const_ minval expr), bitsize bounds )

-- Makes a single Vexp out of zero, one or several input vectors.
-- Zero vectors retorns a single group key. one vector returns the vector itself.
-- It makes new keys (left to right) by concatenating them bitwise.
-- and Error is returned if there would be information loss.
makeCompositeKey :: (Vexp,ColInfo) -> [(Vexp, ColInfo)] -> Either String (Vexp, ColInfo)
makeCompositeKey ( contextV, ColInfo {count=contextcount} ) inputs =
  let normalizedInputs = map normalize inputs in
      do let initV = (zeros_ contextV, 0)
         (outKey,outbitsize) <- foldM composeKeys initV normalizedInputs
         --bitsize 0 => max = 0 bitsize 1 => max = 1
         let maxout = (1 `shiftL` (fromInteger $ toInteger outbitsize)) - 1
         let hintedOutKey = outKey &. const_ maxout outKey --used as a hint to Voodoo (to infer size)
         return $ (hintedOutKey, ColInfo { bounds=(0, maxout), count=contextcount}) --the count is the count of entries

composeKeys :: (Vexp,Int64) -> (Vexp,Int64) -> Either String (Vexp,Int64)
composeKeys (oldkey,oldbits) (deltakey,deltabits) =
  let newbits = oldbits + deltabits
  in do check newbits (<= 32) "cannot compose keys to something larger than 32 bits"
        let shiftold = oldkey <<. (const_  deltabits oldkey)
        let newkey = shiftold |. deltakey
        return $ (newkey, newbits)


groups_ :: Int64 -> (Vexp,ColInfo) -> (Vexp,ColInfo)
groups_ groupsizelg (v,ColInfo {count})
  = let gv = (pos_ v) >>. (const_ groupsizelg v)
    in (gv, ColInfo {bounds=(0, count `shiftR` (fromInteger $ toInteger groupsizelg)), count})

make2LevelFold :: Config -> FoldOp -> (Vexp,ColInfo) -> Vexp -> Either String Vexp
make2LevelFold config foldop groupinfo fdata =
  do let level1par = groups_ (grainsizelg config) groupinfo
     (level1groups, _) <- (makeCompositeKey groupinfo [groupinfo, level1par]) -- meant to be parallel
     let level1results  = Fold { foldop, fgroups = level1groups, fdata }
     (level2groups, _) <- makeCompositeKey groupinfo [groupinfo]  -- meant to be sequential
     let level2results  = Fold { foldop, fgroups = level2groups, fdata = level1results }
     return level2results

-- solutions: every Vexp has attached counts and bounds (put it in the type) (only how to repr it. right now: table)
-- may want to print these things in the vlite repr to track/debug them => table gets in the way of that.
-- solutions: transformation that takes a vexp and derives its attributes
         -- advantages: deals gracefully with intermediates added by explicitly (eg ranges, binops)
         -- disadvantages? :  what if we know of special cases at the binary layer that are harder to match
         -- at the vlite layer?
-- make the easy-to-write binary cases also track sizes. This allows one to make literals that track their
-- inputs properties
