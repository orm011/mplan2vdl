module Vlite( vexpsFromMplan
            , Vexp(..)
            , UniqueSpec(..)
            , Vx(..)
            , BinaryOp(..)
            , ShOp(..)
            , FoldOp(..)
            , Lineage(..)
            , CPVariant(..)
            , xformIden
            , redundantRangePass
            , algebraicIdentitiesPass
            , loweringPass
            , pos_
            , complete) where


import Control.Monad.Reader
import Config
import Types
import qualified Mplan as M
import Mplan(BinaryOp(..))
import Name(Name(..))
import qualified Name as NameTable
--import Control.Monad(foldM)
import Data.List (foldl',(\\))
import Prelude hiding (lookup) {- confuses with Map.lookup -}
import GHC.Generics
import Control.DeepSeq(NFData)
import Data.Int
import Data.Either
--import qualified Error as E
--import Error(check)
import Data.Bits
--import Debug.Trace
import Text.Groom
import qualified Data.HashMap.Strict as Map
--import qualified Data.Map.Strict as Map
--import Data.HashMap.Strict((!))
--import Data.String.Utils(join)
import Data.List.NonEmpty(NonEmpty(..))
import qualified Data.List.NonEmpty as N

--type VexpTable = Map Vexp Vexp  --used to dedup Vexps
import Text.Printf
import Control.Exception.Base (assert)
--import qualified Data.Map.Strict as Map
--import qualified Data.Set as Set

import Data.Hashable
--import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C
import Sha

type Map = Map.HashMap
type NameTable = NameTable.NameTable

data ShOp = Gather | Scatter
  deriving (Eq, Show, Generic)
instance NFData ShOp
instance Hashable ShOp

data FoldOp = FSum | FMax | FMin | FSel | FChoose
  deriving (Eq, Show, Generic)
instance NFData FoldOp
instance Hashable FoldOp

(|>) :: a -> (a -> b) -> b
(|>) x f = f x

-- a hacky implementation of sha1 for Vx variants
sha1vx :: Vx -> SHA1 -- trying to not be as weak as the plain hash
--  variants with no recursion can be sha1'd easily:
sha1vx vx@(Load _) = sha1 $ C.pack (show vx)
sha1vx vx@(RangeC {}) = sha1 $ C.pack (show vx)
sha1vx vx = sha1hack vx

scatteredToWithHint :: Vexp -> Vexp -> Vexp
values `scatteredToWithHint` positions =
  let shpos = addScatterSizeHint positions
  in complete $ Shuffle { shop=Scatter, shsource=values, shpos, shshape=Nothing }

scatteredTo :: Vexp -> Vexp -> Vexp
values `scatteredTo` positions = complete $ Shuffle { shop=Scatter, shsource=values, shpos=positions, shshape=Nothing }

(@@) :: Vexp -> Vexp -> Vexp
values @@ positions  = complete $ Shuffle { shop=Gather, shsource=values, shpos=positions, shshape=Nothing }

crossp :: Vexp -> Vexp -> (Vexp, Vexp)
left `crossp` right =
  let outer_idx = CrossProduct { left, right, variant=COuter }
      inner_idx = CrossProduct { left, right, variant=CInner }
  in (complete outer_idx, complete inner_idx)

tfScatterTo :: Vexp -> (Vexp,Vexp) -> Vexp
values `tfScatterTo` (positions,shape) = complete $ Shuffle { shop=Scatter, shsource=values, shpos=positions, shshape=Just shape }

data CPVariant = COuter | CInner deriving (Show,Eq,Generic)
instance NFData CPVariant
instance Hashable CPVariant

data Vx =
  Load Name
  | RangeV { rmin :: Integer, rstep :: Integer, rref::Vexp }
  | RangeC { rmin :: Integer, rstep :: Integer, rcount::Integer}
  | Binop { binop :: BinaryOp, left :: Vexp, right :: Vexp }
  | Shuffle { shop :: ShOp, shsource :: Vexp, shpos :: Vexp, shshape :: Maybe Vexp } -- shape used for TF
  | Fold { foldop :: FoldOp, fgroups :: Vexp, fdata :: Vexp }
  | Semisort { sdata :: Vexp }
    -- returns a permutation such that when the input is gathered with it,
    -- the output has all instances of an equal value be contiguous
  | Partition { pivots:: Vexp, pdata::Vexp }
  | Like { ldata::Vexp, lpattern::C.ByteString, lcol::Name }
  | VShuffle { varg :: Vexp }
  | CrossProduct { left::Vexp, right::Vexp, variant::CPVariant  } -- can be the outer vec or the inner vec
  deriving (Eq,Generic)
instance NFData Vx
instance Hashable Vx
instance Show Vx where
  show (Load n) = "Load " ++ show n
  show RangeC {} = "RangeC {...}"
  show Binop {binop} = "Binop { binop=" ++ show binop ++ " left=..., right=... }"
  show RangeV {rmin, rstep} = "RangeV { rmin=" ++ show rmin ++ ", rstep=" ++ show rstep ++ ", rref=... }"
  show Shuffle {shop} = "Shuffle { shop=" ++ show shop ++ ", shsource=..., shpos=... }"
  show Fold {foldop} = "Fold { foldop=" ++ show foldop ++ ", fgroups=..., fdata=... }"
  show Partition {} = "Partition {pivots=..., pdata=...}"
  show Like {lpattern} = "Like {ldata=..., lpattern=" ++ show lpattern ++ " }"
  show VShuffle {} = "VShuffle{...}"
  show Semisort {} = "Semisort{...}"
  show CrossProduct {} = "CrossProduct{...}"

data UniqueSpec = Unique | Any deriving (Show,Eq,Generic)
instance NFData UniqueSpec
instance Hashable UniqueSpec

data Lineage = Pure {col::Name, mask::Vexp} | None  deriving (Generic)
instance Show Lineage where
  show None = "None"
  show Pure {col} = "Pure { col = " ++ show col ++ ", mask=... }"

instance NFData Lineage

data Vexp = Vexp { vx::Vx
                 , info::ColInfo
                 , lineage::Lineage
                 , name::Maybe Name
                 , memoized_hash::SHA1
                 , quant::UniqueSpec
                 , comment::C.ByteString
                 } deriving (Show,Generic)

instance Hashable Vexp where
  hashWithSalt s (Vexp{memoized_hash}) = hashWithSalt s (show memoized_hash)

instance Eq Vexp where
  -- hope for the best that the hashing im doing is good enough
  (==) Vexp{memoized_hash=hs1} Vexp{memoized_hash=hs2} = hs1 == hs2

-- if lineage is set to somthing, it means the
-- column values all come untouched from an actual table column (as opposed to being derivative)
-- however, the values may have been shuffled/duplicated in different ways. the Vexp shows if this is so.
-- basically, the following identity holds: vx = gather gathermask=lineagevx data=tableName
-- if the uniqueSpec is unique, then it means that the column has not duplicated values wrt
-- the original one.
-- Note also: right now, uniqueSpec means conditionally unique: if the original column was unique (such as the pos id)
-- then the current version is also unique.
instance NFData Vexp

-- instance Monad W where
--   W vx  >>= fn = fn vx
--   return a = W a
--- Note: if I want vectors to keep their names, then all of these functions
--- that make a new vector need to have names for their inner vectors.

{- some convenience vectors -}
pos_ :: Vexp -> Vexp
pos_ v = complete $ RangeV {rmin=0, rstep=1, rref=v}

const_ :: Integer -> Vexp -> Vexp
const_ k v = complete $ RangeV {rmin=k, rstep=0, rref=v}

-- used for literals, so they don't lose their type info.
typedconst_ :: Integer -> Vexp -> DType -> Vexp
typedconst_ k v dt = let partial@Vexp{info} = const_ k v
                         newinfo = checkColInfo $ info{stype=SInt32, dtype=(dt, "literal")}
                     in partial {info=newinfo}
zeros_ :: Vexp -> Vexp
zeros_ = const_ 0

ones_ :: Vexp -> Vexp
ones_ = const_ 1

(==.) :: Vexp -> Vexp -> Vexp
a ==. b = makeBinop Eq a b

(>.) :: Vexp -> Vexp -> Vexp
a >. b = makeBinop Gt a b

(<.) :: Vexp -> Vexp -> Vexp
a <. b = makeBinop Gt b a -- notice switch

(>>.) :: Vexp -> Vexp -> Vexp
a >>. b = makeBinop BitShift a b

(<<.) :: Vexp -> Vexp -> Vexp -- BitShift uses sign to encode direction
a <<. b = let z = zeros_ b
              negb = (z -. b)
          in a >>. negb

(||.) :: Vexp -> Vexp -> Vexp
a ||. b = makeBinop LogOr a b

(|.) :: Vexp -> Vexp -> Vexp
a |. b = makeBinop BitOr a b

(&.) :: Vexp -> Vexp -> Vexp
a &. b = makeBinop BitAnd a b

(-.) :: Vexp -> Vexp -> Vexp
a -. b = makeBinop Sub a b

(*.) :: Vexp -> Vexp -> Vexp
a *. b = makeBinop Mul a b

(+.) :: Vexp -> Vexp -> Vexp
a +. b = makeBinop Add a b

(/.) :: Vexp -> Vexp -> Vexp
a /. b = makeBinop Div a b

(%.) :: Vexp -> Vexp -> Vexp
a %. b = makeBinop Mod a b

makeBinop :: BinaryOp -> Vexp -> Vexp -> Vexp
makeBinop binop left right = complete $ Binop {binop, left, right}

(?.) :: Vexp -> (Vexp,Vexp) -> Vexp
cond ?. (a,b) =  let  ones = ones_ cond
                      zeros = zeros_ cond
                      negcond = cond ==. zeros
                      -- need to make condition boolean for mult.
                      poscond = ones -. negcond
                      left = poscond *. a
                      right = negcond *. b
                 in left +. right

complete :: Vx -> Vexp
complete vx =
  let info = checkColInfo $ inferMetadata vx
      lineage= checkLineage $ inferLineage vx
      quant = inferUniqueness vx
      memoized_hash = sha1vx vx
      name = case vx of
        Shuffle {shsource=Vexp{name=orig_name}} -> orig_name -- preserve name for scatter and gather.
        _ -> Nothing
      comment = ""
  in Vexp { vx, info, lineage, name, memoized_hash, quant, comment }


checkLineage :: Lineage -> Lineage
checkLineage l =
  case l of
    None -> None
    Pure _ Vexp {lineage, name} ->
      case (lineage,name) of
        (None, Nothing) -> l
        _ -> error "lineage vector should not itself have lineage or name"

inferMetadata :: Vx -> ColInfo
-- typeNeeded :: (Integer,Integer) -> SType
-- typeNeeded (lower,upper) =
--   if lower < (toInteger  (minBound :: Int32))
--      || upper > (toInteger  (maxBound :: Int32))
--   then if lower < (toInteger  (minBound ::Int64))
--           || upper > (toInteger  (maxBound ::Int64))
--           then error "range does not fit in a 64 bit int"
--        else SInt64
--   else SInt32

-- two arrays:
-- 0,1,2,3 X 0,1 = 0,0,1,1,2,2,3,3 (outer)
--                 0,1,0,1,0,1,0,1 (inner) length is the same. values range differently.
inferMetadata CrossProduct { left, right, variant }
  = let Vexp {info=ColInfo {bounds=outerbounds, count=outercount}} = (pos_ left)
        Vexp {info=ColInfo {bounds=innerbounds, count=innercount}} = (pos_ right)
        bounds = case variant of -- the max values depend on whether it is inner or outer
          COuter -> outerbounds
          CInner -> innerbounds
    in ColInfo {bounds, count=outercount*innercount, stype=SInt32, dtype=(DDecimal{point=0}, ""), trailing_zeros=0}


inferMetadata (Load _) = error "at the moment, should not be called with Load. TODO: need to pass config to address this case"

inferMetadata VShuffle {varg=Vexp{info}} = info -- same, just no ordering if there was

inferMetadata Like { ldata=Vexp{info=ColInfo{count}} }
  = ColInfo { bounds=(0,1), count, stype=SInt32, trailing_zeros=0, dtype=(DDecimal{point=0},"") }

inferMetadata RangeV {rmin=rstart,rstep,rref=Vexp {info=ColInfo {count}}}
  =  let extremes = [rstart, rstart + count*rstep]
         bounds = (minimum extremes, maximum extremes)
     in ColInfo { bounds
                , count
                , stype=SInt64
                , dtype=(DDecimal{point=0},"")
                , trailing_zeros = 0}

inferMetadata RangeC {rmin=rstart, rstep, rcount}
  =  let extremes = [rstart + rcount*rstep, rstart]
     in ColInfo { bounds=(minimum extremes, maximum extremes)
                , count=rcount
                , stype=SInt64
                , dtype=(DDecimal{point=0},"")
                , trailing_zeros = 0}

inferMetadata Shuffle { shop=Scatter
                      , shsource=Vexp {info=ColInfo {bounds=sourcebounds, stype, dtype, trailing_zeros}}
                      , shpos=Vexp {info=ColInfo {bounds=(_,posmax)}}
                      }
  = ColInfo { bounds=sourcebounds, count=posmax, stype, dtype, trailing_zeros }

inferMetadata Semisort { sdata = Vexp {info} } -- keeps all current metadata the same.
  = info

inferMetadata Shuffle { shop=Gather
                      , shsource=Vexp {info=ColInfo {bounds=sourcebounds, stype, dtype, trailing_zeros}}
                      , shpos=Vexp {info=ColInfo {count}}
                      }
  = ColInfo { bounds=sourcebounds, count, stype, dtype, trailing_zeros }

inferMetadata Fold { foldop=FSel
                   , fgroups=_
                   , fdata=Vexp {info=ColInfo {count}}
                   }
  = ColInfo {bounds=(0, count-1), count, stype=SInt64, dtype=(DDecimal{point=0},""), trailing_zeros=0 } -- positions always int64

inferMetadata Fold { foldop
                   , fgroups = Vexp { info=ColInfo {bounds=(glower,gupper), count=gcount} }
                   , fdata = Vexp { info=ColInfo {bounds=(dlower,dupper), count=dcount, stype, dtype=(dt,_), trailing_zeros} }
                   } =
  -- "suspicious: group and data count bounds dont match"
  --assert (gcount == dcount) $ -- TODO is this assert correct?
  let count_bound = min (gupper - glower + 1) gcount -- cannot be more outputs than distinct group values
  in case foldop of
    FSum -> let extremes = [dlower, dlower*dcount, dupper, dupper*dcount]
                dtypeout = case dt of
                  DDecimal {point} -> DDecimal{point}
                  DDate  -> DDecimal{point=0} -- reinterpret
                  DString {} -> DDecimal{point=0} -- reinterpret
                  -- for positive dlower, dlower is the minimum.
                  -- for negative dlower, dlower*dcount is the minimum, and so on.
            in ColInfo { bounds=(minimum extremes, maximum extremes), count=count_bound, stype, dtype=(dtypeout,""), trailing_zeros }
    FMax -> ColInfo { bounds=(dlower, dupper), count=count_bound, stype, dtype=(dt,""), trailing_zeros}
    FMin -> ColInfo { bounds=(dlower, dupper),  count=count_bound, stype, dtype=(dt,""), trailing_zeros }
    FChoose -> ColInfo { bounds=(dlower, dupper),  count=count_bound, stype, dtype=(dt,""), trailing_zeros }
    FSel -> error "use different handler for select (should be above this one)"

-- the result of partition is a list of indices that, for each pdata
-- tells it where the first element in pivots is that is larger or equal to it.
-- so, the outputsize is the same as that of pdata
-- and, if there is a bound for each element in the input,
-- the output values are in the domain [0..(count pivots  - 1)]
inferMetadata Partition
  { pivots=Vexp { info=ColInfo {count=pivotcount} }
  , pdata=Vexp { info=ColInfo {count=datacount} }
  } = ColInfo {bounds=(0,pivotcount-1), count=datacount, stype=SInt64, dtype=(DDecimal{point=0},""), trailing_zeros=0 }


inferMetadata arg@Binop { binop
                        , left=Vexp { info=ColInfo {count=c1 , stype=ltype, dtype=(ldtype,_), trailing_zeros=ltrail } }
                        , right=Vexp { info=ColInfo {count=c2, stype=rtype, dtype=(rdtype,_), bounds=(_, upper)} }
                        } =
    let count = min c1 c2
        bounds = inferBounds arg
        trailing_zeros' = case binop of
          BitShift -> (ltrail - upper)
          _ -> 0
        stype = case (binop,ltype,rtype) of
          (Mul,SDecimal{precision=lp, scale=ls},SDecimal{precision=rp,scale=rs}) -> SDecimal{precision=lp + rp, scale=ls + rs}
          (Mul,SDecimal{},SInt32) -> ltype
          (Mul,SDecimal{},SInt64) -> ltype
          (Mul,_,SDecimal{}) -> rtype
          (Div, SDecimal{}, SInt32) -> ltype
          (Div, SDecimal{}, SInt64) -> ltype
          (Div, SDecimal{precision=lp, scale=ls},
           SDecimal{precision=rp, scale=rs}) ->
            let diff = ls - rs
            in if diff >= 0
               then SDecimal {precision = max lp rp, scale=diff} -- why max?
               else error "implement division where numerator dec. point is less than denominator"
          _ -> ltype
        errMsg msg cs = C.intercalate " " ["ERROR", msg, C.pack $ show cs]
        warnMsg msg cs =  C.intercalate " " ["WARNING", msg, C.pack $ show cs]
        dtype = let cs = (binop,ldtype,rdtype) in
          case cs of
            (Mul,DDecimal{point=ls},DDecimal{point=rs}) -> (DDecimal{point=ls + rs},"")
            (Div,DDecimal{point=ls},DDecimal{point=rs}) ->
              let diff = ls - rs
              in if diff >= 0
                 then (DDecimal {point=diff}, "")
                 else error "need to implement conversion for this division"
            (cmp,_,_)
              | cmp `elem` [Gt, Lt, Leq, Geq, Eq, Neq] && ldtype == rdtype
                -> (DDecimal{point=0},"")
            (cmp,_,_)
              | cmp `elem` [Gt, Lt, Leq, Geq, Eq, Neq]
                -> (ldtype, errMsg "comparing across types without conversion" cs)
            (arith,DDecimal{point=ls},DDecimal{point=rs})
              | arith `elem` [Sub, Add]
                -> if ls == rs then (ldtype,"")
                   else (ldtype,
                         errMsg "addition across different types without conversion" cs)
            _ -> (ldtype, warnMsg "case not implemented:" cs)
    in ColInfo {bounds, count, stype, dtype, trailing_zeros=trailing_zeros'} -- arbitrary choice of type right now.
         -- until we need more precision, we're conservative on the trailing zeros...

inferBounds :: Vx -> (Integer, Integer)
inferBounds Binop { binop
  , left=left@Vexp { info=ColInfo {bounds=(l1,u1)} }
  , right=right@Vexp { info=ColInfo {bounds=(l2,u2) } }
  } = case binop of
       Gt ->  (0,1)
       Lt ->  (0,1)
       Eq ->  (0,1)
       Neq ->  (0,1)
       Geq ->  (0,1)
       Leq ->  (0,1)
       LogAnd ->  (0,1)
       LogOr ->  (0,1)
       Add ->  (l1 + l2, u1 + u2)
       Sub ->  (l1 - u2, u1 - l2) -- notice swap
       Mul -> let allpairs = sequence [[l1,u1],[l2,u2]] -- cross product
                  prods = map (foldl (*) 1) allpairs
              in  (minimum prods, maximum prods) -- TODO double check reasoning here.
       Div -> let allpairs = [(l1,l2), (l1,u2), (u1,l2), (u1,u2)]
                  divs = map (\(x,y) -> x `div` y) allpairs
              in  (minimum divs, maximum divs) -- TODO double check reasoning here.
       Min -> (min l1 l2, min u1 u2) -- this is true, right?
       Max -> (max l1 l2, max u1 u2)
       Mod -> (0, u2-1) -- assuming mods are always positive
       BitAnd -> if (l1 >= 0 && l2 >= 0)
                 then let mx = min (maxForWidth left) (maxForWidth right)
                      in (0,mx) -- precision could be improved.
                 else (toInteger $ (minBound :: Int64), toInteger$ (maxBound :: Int64)) -- used to be an error
       BitOr -> if (l1 >= 0 && l2 >= 0)
                then let mx = max (maxForWidth left) (maxForWidth right)
                     in (0,mx) -- precision could be improved.
                else (toInteger $ (minBound :: Int64), toInteger $ (maxBound :: Int64)) -- used to be an error
       BitShift -> --assert (u2 < 64) $ --"shift left by more than 31?"
                   --    -- shift left is like multiplication (amplifies neg and pos)
                   --    -- shift right is like division (shrinks numbers neg and pos)
                   --    -- both can happen in a single call to shift...
                   let mshift (a,b) = let shfmask = (fromInteger $ toInteger $ abs b)
                                      in if b < 0 then (a `shiftL` shfmask)
                                         else a `shiftR` shfmask
                       allpairs = [(l1,l2), (l1,u2), (u1,l2), (u1,u2)]
                       extremes = map mshift allpairs
                   in (minimum extremes, maximum extremes)
             -- type_compatible = case (lefttype, righttype) of -- this check is quite loose.
             --   (a,b) | a == b -> True -- identical then true.
             --   (SChar _, SChar _) -> True -- character types.
             --   (SInt64, SInt32) -> True -- C is good enough for this
             --   (SInt32, SInt64) -> True -- C is good enough for this
             --   _ -> error $ "incompatible types for op: " ++ show binop ++ "\nleft:\n" ++ show left ++ "\nright:\n" ++ show right
               -- notice incompatible decimals will fail here

inferBounds _ = error "not for this variant" -- not needed or called for other variants

inferLineage :: Vx -> Lineage

-- note: we are assuming the scatter covers everything.
-- gather and scatter preserve lineage.
inferLineage Shuffle { shop
                     , shsource=Vexp {lineage=Pure{col, mask=lineagev}}
                     , shpos
                     , shshape
                     } =
  Pure  { col
        , mask=complete $ Shuffle { shop
                                  , shsource=lineagev
                                  , shpos
                                  , shshape
                                  }
        }

-- Foldmin, Foldmax preserve lineage
inferLineage Fold { foldop
                  , fgroups
                  , fdata = Vexp { lineage = Pure {col, mask=lineagev} }
                  }
  | (foldop == FMin) || (foldop == FMax) || (foldop==FChoose) =
      Pure {col, mask=complete $ Fold {foldop, fgroups, fdata=lineagev} }

inferLineage _ = None

inferUniqueness :: Vx -> UniqueSpec

-- TODO: if scatter does not cover everything then this is wrong.
inferUniqueness Shuffle { shop=Scatter
                        , shsource=Vexp {quant}
                        } = quant

inferUniqueness Shuffle { shop=Gather
                        , shsource=Vexp{quant}
                        , shpos=Vexp{quant=Unique}
                        } = quant

inferUniqueness Partition { } = Unique

-- strictly unique
inferUniqueness RangeV {rstep}
  | rstep /= 0 = Unique

inferUniqueness RangeC {rstep}
  | rstep /= 0 = Unique

inferUniqueness Fold { foldop=FSel } = Unique

--inferUniqueness (Load _) -- would need to pass config for ths
inferUniqueness _ = Any

vexpsFromMplan :: M.RelExpr -> Config -> [Vexp]
vexpsFromMplan r c  = solve' c r

outputName :: (M.ScalarExpr, Maybe Name) -> Maybe Name
outputName (_, Just alias) = Just alias {-alias always wins-}
outputName ((M.Ref orig), Nothing) = Just orig {-anon still has name if ref-}
outputName _ = Nothing

-- includes both the list of all entries, as well
-- as a lookup friendly map
data Env = Env [Vexp] (NameTable Vexp)

makeEnv :: [Vexp] -> Env
makeEnv lst = Env lst (makeTable lst)
  where makeTable pairs = foldl' maybeadd NameTable.empty pairs
        maybeadd env Vexp { name=Nothing } = env
        maybeadd env vexp@Vexp { name=(Just newalias)} =
          NameTable.insert newalias vexp env

makeEnvWeak :: [Vexp] -> Env
makeEnvWeak lst =
  let tbl = makeTable lst
  in Env lst tbl
  where makeTable pairs = foldl' maybeadd NameTable.empty pairs
        maybeadd env Vexp { name=Nothing} = env
        maybeadd env vexp@Vexp { name=(Just newalias) } =
          NameTable.insertWeak newalias vexp env

addComment :: C.ByteString -> [Vexp] -> [Vexp]
addComment str exps  =
  map (\e -> e {comment=C.intercalate " " [(comment e),  str]}) exps

{- helper function that makes a lookup table from the output of a previous
operator. the lookup table loses information about the anonymous outputs
of the operator, so it only makes sense to use this in internal nodes
whose vectors will be consumed by other operators, but not on the
top level operator, which may return anonymous columns which we will need to
keep around -}


solve :: Config -> M.RelExpr -> Env
solve config relexp = solve' config relexp |> checkVecs |> makeEnv
  where checkVecs vexplist =
          let sizes = map (count . info) vexplist
              minsize = minimum sizes
              maxsize = maximum sizes
          in assert (minsize == maxsize) vexplist

solve' :: Config -> M.RelExpr -> [Vexp]

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
  do (colnam, alias) <- tablecolumns
     return $ loadAs config tablename colnam alias

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
  let addEntry (listz, acclist) tup = (listz, tup : acclist) -- adds output to acclist
      foldFun lsts@(list0, acclist) arg@(expr, _) =
        let asenv  = makeEnvWeak $ list0 ++ acclist-- use both lists for name resolution
           -- allow collisions b/c they do happen .
            anon  = fromScalar asenv expr --either monad
        in addEntry lsts $ anon {name=outputName arg}
      (Env l0 _) = solve config child -- either monad -- used for reading
      (_ , solved) =  foldl' foldFun (l0,[]) projectout
  in solved


-- important cases to consider:
-- alias in the keyname: groupby [a as b][sum(a), b]
solve' config M.GroupBy { M.child,
                          M.inputkeys,
                          M.outputaggs } =
  case solve config child of --either monad
    (Env list0@(refv:_) nt) ->
      let (keys,aliases) = unzip inputkeys
          keyvecs  = map (\n -> snd $ (NameTable.lookup_err n nt)) keys
          keyaliases = do (v,malias) <- zip keyvecs aliases -- list monad
                          case malias of
                            Nothing -> []
                            n -> [v { name=n }]
          list1 = list0 ++ keyaliases
          gbkeys = case keyvecs of
            [] -> let z@(Vexp {info=ColInfo{bounds}}) = zeros_ refv
                  in assert (bounds == (0,0)) $ z :| []
            a : rest -> a :| rest
          gkey@Vexp { info=ColInfo {bounds=(gmin, _)}}  =
            let ans@Vexp {info=ColInfo{bounds}} = runReader (makeCompositeKey gbkeys) config
            in assert (True || inputkeys /= [] || bounds == (0,0) ) (ans{comment="groupBy key"})
          solveSingleAgg env after_env pr@(agg, _) = -- before columns must be groups. after ones are already grouped
              let anon@Vexp{ quant=orig_uniqueness, lineage=orig_lineage } = solveAgg config env after_env gkey agg
                  outalias = case pr of
                    (M.GFold M.FChoose  (M.Ref n), Nothing) -> Just n
                    (_, alias) -> alias
                    _ -> Nothing
                  out_uniqueness = case (keys, pr) of
                    ([gbk], (M.GFold M.FChoose (M.Ref n), _)) -- if there is a single gb key the output version of that col is unique
                      | n == gbk -> Unique  -- right now, we only do this when the input key has a lineage. but it neednt.
                    _ -> orig_uniqueness
                  out_lineage = case orig_lineage of
                    None -> None
                    Pure { col, mask=orig_mask@Vexp{quant=mask_uniqueness}} ->
                      let out_mask_quant = if out_uniqueness == Unique
                                           then Unique
                                           else mask_uniqueness
                      in Pure {col, mask=orig_mask {quant=out_mask_quant}}
              in anon {name=outalias, quant=out_uniqueness, lineage=out_lineage }
          addEntry (x, acclist) tup = (x, tup : acclist) -- adds output to acclist
          foldFun lsts@(x, acclist) arg =
            let env = makeEnvWeak $ x ++ acclist-- use both lists for name resolution
                after_env = makeEnv acclist
                ans@Vexp {info=ColInfo{count} } = solveSingleAgg env after_env arg --either monad
            in addEntry lsts $ assert (True || inputkeys /= [] || count == 1) $  ans
          (_, final) = assert (gmin == 0) $ foldl' foldFun (list1,[]) outputaggs
      in addComment "groupBy output" final
    Env [] _ -> error "empty env"


solve' config M.CartesianProduct { M.leftch
                                 , M.rightch
                                 } =
  let Env left_vecs _ = solve config leftch
      Env right_vecs _  = solve config rightch
      (outer_idx, inner_idx) = (head left_vecs) `crossp` (head right_vecs)
      left_prod = gatherAll left_vecs outer_idx
      right_prod = gatherAll right_vecs inner_idx
  in left_prod ++ right_prod

solve' config M.Join { M.leftch
                     , M.rightch
                     , M.conds
                     , M.joinvariant
                     } =
  let sleft@(Env colsleft _) = solve config leftch
      sright@(Env colsright _) = solve config rightch
  in case separateFKJoinable config (N.toList conds) sleft sright of
    ([jspec@FKJoinSpec{joinorder}],[]) -> case joinorder of
      FactDim -> handleGatherJoin config sleft sright joinvariant jspec
      DimFact -> handleGatherJoin config sright sleft joinvariant jspec
    ([jspec@SelfJoinSpec{}],[]) -> handleGatherJoin config sleft sright joinvariant jspec
    ([], [M.Binop{ M.binop
                   , M.left=leftexpr
                   , M.right=rightexpr }])
        | keycol_left <- sc sleft leftexpr
        , keycol_right <- sc sright rightexpr
        , (1, [_]) <- (count $ info keycol_left, colsleft)
          -> let broadcastcol_left = keycol_left @@ (zeros_ keycol_right)
                 boolean = complete $ Binop {binop, left=broadcastcol_left, right=keycol_right}
                 gathermask = complete $ Fold {foldop=FSel, fgroups=pos_ boolean, fdata=boolean}
             in gatherAll colsright gathermask
    ([], [M.Binop{ M.binop
                  , M.left=leftexpr
                  , M.right=rightexpr }])
        | keycol_left <- sc sleft leftexpr
        , keycol_right <- sc sright rightexpr
        , (1,[_]) <- (count $ info keycol_right, colsright)
          ->  let broadcastcol_right = keycol_right @@ zeros_ keycol_left
                  boolean = complete $ Binop {binop, left=keycol_left, right=broadcastcol_right}
                  gathermask = complete $ Fold {foldop=FSel, fgroups=pos_ boolean, fdata=boolean}
              in addComment "join output" $ gatherAll colsleft gathermask
    ([_],more@[extra]) -> -- single condition on each side. could do more on the right but need to AND them.
        if joinvariant == M.Plain
        then solve' config M.Select { M.child=M.Join {M.leftch=leftch, M.rightch=rightch, M.conds=N.fromList (N.toList conds \\ more), M.joinvariant=joinvariant}
                                    , M.predicate=extra }
        else error "can only do this rewrite for plain joins" -- left outer joins would be wrong.
    ow -> error $ "not handling this join case right now: " ++ show ow

solve' config M.Select { M.child -- can be derived rel
                       , M.predicate
                       } =
  let childenv@(Env childcols _) = solve config child -- either monad
      fdata  = sc childenv predicate
      fgroups = pos_ fdata
      idx = complete $ Fold {foldop=FSel, fgroups, fdata}
  in do unfiltered@Vexp { name=preserved }  <- childcols -- list monad
        return $ let sel = unfiltered @@ idx
                 in ( sel {name=preserved} )

solve' _ r_  = error $ "unsupported M.rel:  " ++ groom r_

getRefVector :: Config -> Name -> Vexp
getRefVector config tablename =
  let pkname = lookupPkey config tablename
      (_,pkinfo) = NameTable.lookup_err pkname (colinfo config)
      pkvx=Load pkname
  in case format config of
    VliteFormat -> complete $ RangeC {rmin=0, rcount=count pkinfo, rstep=1}
    VdlFormat -> Vexp{vx=pkvx, info=pkinfo, lineage=None, memoized_hash=sha1vx pkvx, quant=Unique, name=Nothing, comment="ref vector"} -- this vector is used only for the ref part of other columns

loadAs :: Config -> Name -> Name -> Maybe Name -> Vexp
loadAs config tablename colname alias =
  let mask = pos_ (getRefVector config tablename)
      outname = case alias of
        Nothing -> Just colname
        Just x -> Just x
  in case colname of
    Name [_,"%TID%"] -> mask { lineage=Pure {col=colname, mask}, name=outname } -- the mask is already good, but lacks name and lineage
    Name [_,_] -> let (_,clinfo) = NameTable.lookup_err colname (colinfo config)
                      clquant = if (isPkey config (colname:|[]) /= Nothing) then Unique else Any
                      clvx=Load colname
                  in Vexp {vx=clvx, info=clinfo, quant=clquant, lineage=Pure {col=colname, mask}, memoized_hash=sha1vx clvx, name=outname, comment=""} -- this is just for type compatiblity.
    Name _ -> error "unexpected name"


-- all the names share the same mask
-- the gquant reports if put together, each row of the resulting composite is unique
-- (for now, the deduction of that property only works for trivial cases, used in query 18)


-- split these conditions into categories: those that match an fk relation get resolved, the rest get put on a separate side
separateFKJoinable :: Config -> [M.ScalarExpr] -> Env -> Env -> ([JoinSpec], [M.ScalarExpr])
separateFKJoinable config conds (Env _ leftEnv) (Env _ rightEnv) =
  let joinEnv = (let mleft = map (\(n,a) -> (n, (LeftChild, a))) (NameTable.toList leftEnv)
                     mright = map (\(n,a) -> (n, (RightChild,a))) (NameTable.toList rightEnv)
                     mergedlist = (mleft ++ mright)
                     mergedmap = NameTable.fromList mergedlist
                 in assert (length mergedlist == (length $ NameTable.toList mergedmap)) $ mergedmap) -- check the merging loses no entries
      foldFun (partials::Partials, non::[M.ScalarExpr]) expr =
        let (partials', mexp) = classifyExpr config partials joinEnv expr
        in (partials', mexp ++ non)
      (finalpartial, finalnon) = foldl' foldFun (Map.empty, []) conds
      -- pcols in partial fk joinspec holds the full set of pairs of columns needed to complete the join
      -- pjoinorder holds how fact and dim map to our left and right arguments (they could go either way)
      partialSpecToJoinSpec (PartialFKJoinSpec {pfactmask, pcols, pdimmask, pjoinorder}, (AccFK (kp,quant),origs)) =
        if kp == pcols
        then case isFKRef config kp of
          Just (FKInstance {fkjoinorder=whichl, idxname=joinidx, dim=dimname}) ->
            let final = FKJoinSpec { factmask=pfactmask {comment="factmask"}, dimmask=pdimmask{comment="dimmmask"}, factunique=quant, joinorder=pjoinorder, joinidx,
                                     dimref = getRefVector config dimname }
            in assert (whichl == FactDim) $ Left final
                --because we keep fact on left, then at this point lookup should alwasy by left child. Who was the actual
                --left child is kept in the joinorder field.
          Nothing -> error "kp was result of lookup earlier"
        else assert ((N.length kp ) > (N.length pcols)) $ Right origs
      partialSpecToJoinSpec (PartialSelfJoinSpec {pleftmask, prightmask,ppkcols}, (AccPK (acccols), origs)) =
        if acccols == ppkcols
        then let pkconstraint = case isPkey config acccols of
                   Just x -> x
                   Nothing -> error "at this point this must be a constraint"
             in Left $ SelfJoinSpec {leftmask=pleftmask, rightmask=prightmask, pkconstraint }
        else assert (N.length acccols < N.length ppkcols) $ Right origs
      partialSpecToJoinSpec _ = error "joinspec / acc mismatch"
      finalizePartials p = let entries = Map.toList p
                           in map partialSpecToJoinSpec entries
      (joinspecs,morenons) = partitionEithers $ finalizePartials finalpartial
  in (joinspecs, (foldl' (++) [] morenons) ++ finalnon)

-- maps a name to a vexp, but tags it with information about which side of the join it came from
data WhichChild = LeftChild | RightChild deriving (Eq,Show,Ord)

-- keeps track of the matching column names as well as the mask being used by each side.
data PartialJoinSpec =
  PartialFKJoinSpec
  { pfactmask::Vexp
  , pcols::FKCols
  , pdimmask::Vexp
  , pjoinorder::FKJoinOrder
  }
  | PartialSelfJoinSpec
    { pleftmask::Vexp
    , prightmask::Vexp
    , ppkcols :: PKCols
    }
  deriving (Eq,Show,Generic) -- the left and right are masks.
instance Hashable PartialJoinSpec

-- fully resolve join info. this has all the info needed to compute gather masks without needing
-- make any more config lookups
data JoinSpec
  = FKJoinSpec { factmask::Vexp
               , factunique::UniqueSpec
               , joinidx::Name
               , dimmask::Vexp
               , joinorder::FKJoinOrder
               , dimref :: Vexp
               }
  | SelfJoinSpec { leftmask::Vexp
                 , rightmask::Vexp
                 , pkconstraint::Name
                 }
  deriving (Eq,Show) -- dim mask unique spec must be unique.

data AccCols = AccFK (FKCols, UniqueSpec) | AccPK (PKCols) deriving (Show,Eq)

accmerge :: AccCols -> AccCols -> AccCols
accmerge (AccFK (fkcols1,unique1)) (AccFK (fkcols2,unique2)) =
  AccFK ( N.sort $ N.fromList $ N.toList fkcols1 ++ N.toList fkcols2
        , if unique1==Unique || unique2==Unique then Unique else Any)

accmerge (AccPK pkcols1) (AccPK pkcols2) =
  AccPK (N.sort $ N.fromList $ N.toList pkcols1 ++ N.toList pkcols2)

accmerge _ _ = error "incompatible acc"

type Partials = Map PartialJoinSpec (AccCols, [M.ScalarExpr])

addinfo :: (AccCols, [M.ScalarExpr]) -> (AccCols, [M.ScalarExpr]) -> (AccCols, [M.ScalarExpr])
addinfo (partk1, exprs1) (partk2, exprs2) = (accmerge partk1 partk2, exprs1 ++ exprs2)

-- this is a map that associates a prelim result to a list of accumulated keycols, and the exprs
-- that went into it.

-- either adds condition to the partial, or returns it on the RHS as non-fk join.
classifyExpr :: Config -> Partials -> NameTable (WhichChild, Vexp) -> M.ScalarExpr -> (Partials, [M.ScalarExpr])
classifyExpr config partials joinenv arg@M.Binop{ M.binop=M.Eq
                                                , M.left=M.Ref key1
                                                , M.right=M.Ref key2 } =
  let mpartials = case (snd $ NameTable.lookup_err key1 joinenv, snd $ NameTable.lookup_err key2 joinenv) of -- map should have them, or this is an irrecoverable error.
        ( (LeftChild, Vexp{lineage=Pure{col=leftcol, mask=leftmask}, quant=leftquant})
            , (RightChild, Vexp{lineage=Pure{col=rightcol, mask=rightmask}, quant=rightquant}) )
          -> processPartials config partials (leftcol, leftmask, leftquant) (rightcol,rightmask, rightquant) arg
        ( (RightChild, Vexp{lineage=Pure{col=rightcol, mask=rightmask}, quant=rightquant})
          , (LeftChild, Vexp{lineage=Pure{col=leftcol, mask=leftmask}, quant=leftquant}) )
          -> processPartials config partials (leftcol, leftmask, leftquant) (rightcol, rightmask, rightquant) arg
        _ -> Nothing
  in case mpartials of
    Nothing -> (partials, [arg])
    Just newpartials -> (newpartials, [])

classifyExpr _ partials _ expr = (partials, [expr]) -- not an eq

-- if the pair is potentially part of an fk, then add it. otherwise nothing.
-- TODO: here, also deal with self join case.
processPartials :: Config -> Partials -> (Name, Vexp, UniqueSpec) -> (Name, Vexp, UniqueSpec) -> M.ScalarExpr -> Maybe Partials
processPartials config partials (leftcol, leftmask, leftquant) (rightcol,rightmask,rightquant) expr =
  if leftcol == rightcol
  then case isPartialPk config leftcol of
    Nothing -> Nothing
    Just pks -> if quant leftmask == Unique || quant rightmask == Unique then
                  Just $ Map.insertWith addinfo (PartialSelfJoinSpec { pleftmask=leftmask, prightmask=rightmask, ppkcols=pks })
                  (AccPK (leftcol:|[]), [expr]) partials
                else Nothing
  else case isPartialFk config (leftcol,rightcol) of
    Nothing -> Nothing
    Just (joinorder, kp :: FKCols) ->
      let (deltajoinspec, acc) = case joinorder of
            FactDim -> (PartialFKJoinSpec { pfactmask=leftmask
                                                , pdimmask=rightmask
                                                , pcols=kp
                                                , pjoinorder=FactDim
                                                }
                               , (AccFK ((leftcol,rightcol):|[], leftquant),[expr])) --keep them in (fact,dim) order
            DimFact -> (PartialFKJoinSpec { pfactmask=rightmask
                                                 , pdimmask=leftmask
                                                 , pcols=kp
                                                 , pjoinorder=DimFact
                                                 }
                                , (AccFK ((rightcol,leftcol):|[], rightquant),[expr])) -- fact,dim order
          partials' = Map.insertWith addinfo deltajoinspec acc partials
      in Just partials'


{- makes a vector from a scalar expression, given a context with existing
defintiions -}
fromScalar ::  Env -> M.ScalarExpr  -> Vexp
fromScalar = sc

{- notes about tmp naming in Monet:
 -user columns are only named with lowercase.
 -temporary columns are sometimes named things like L.L or L1.L1.
 -to the right of 'as', we get fully qualified names
 -but sometimes, as a reference, they don't include the fully qualified names.
eg [L1 as L1]. This means we cannot just use a map in those cases, since
a search for L1 should potentially mean L1.L1.
-}
-- sc' :: Env -> M.ScalarExpr -> Vexp
-- sc' env expr =
--   do vx <- sc env expr
--      return $ getInfo vx

sc ::  Env -> M.ScalarExpr -> Vexp
sc (Env _ env) (M.Ref refname)  =
  case (NameTable.lookup refname env) of
    Right (_, v) -> v
    Left s -> error s


sc env (M.Cast {M.mtype = MDouble, M.arg}) = sc env arg -- ignore this cast. it is use only prior to averages etc. note C may not do the right thing.
-- or we should insert a voodoo cast.

-- serious TODO: strictly speaking, I need to know the orignal type in order
-- to correctly produce code here.
-- We need the input type bc, for example Decimal(10,2) -> Decimal(10,3) = *10
-- but Decimal(10,1) -> Decimal(10,3) = * 100. Right now, that input type is not explicitly given.

sc env (M.Cast { M.mtype, M.arg }) =
  let vexp@Vexp{info=ColInfo{dtype=(inputtype,_)}} = sc env arg
      outputtype = getSTypeOfMType mtype
      nm = case inputtype of
        DString {decoder} -> decoder
        _ -> undefined -- we cannot handle casting non-strings to strings (bc what is their dictionary?)
      outdtype = getDTypeOfMType mtype nm
      outrep@Vexp{info} = case (inputtype,outdtype) of
        (a,b) | a == b -> vexp
        -- semantic cast of int to decimal: eg. 1 -> 1.0 which is repr as 10
        (DDecimal {point=sfrom}, DDecimal{point=sto}) ->
          if sto == sfrom then vexp
          else let factor = (10 ^ abs (sto - sfrom))
               in if sto > sfrom
                  then vexp *. const_ factor vexp
                  else vexp /. const_ factor vexp
        _ -> vexp
  in outrep {info=info{stype=outputtype, dtype=(outdtype,"")}}
     -- makes sure the new type is okay. this ensures future casts don't
     -- multiply things by more than needed.

sc env (M.Binop { M.binop, M.left, M.right }) =
  let l@Vexp {info=ColInfo{dtype=(lty,_)}} = sc env left
      r@Vexp {info=ColInfo{dtype=(rty,_)}} = sc env right
  in let ans = case (binop,lty,rty) of
           -- (_, DDecimal{point=lp}, DDecimal{point=rp}) -- handle non-implicit conversions
           --   | binop `elem` [Gt,Lt,Eq,Neq,Geq,Leq]
           --     -> let dest = DDecimal{point=max lp rp}
           --            newl = transformCast l dest
           --            newr = transformCast r dest
           --        in Binop {binop, left=newl, right=newr}
           _ -> Binop {binop, left=l, right=r}
     in complete ans

sc env M.In { M.left, M.set } =
  let sleft  = sc env left
      sset = map (sc env) set
      eqs = map (==. sleft) sset
      (first,rest) = case eqs of
        [] -> error "list is empty here"
        a:b -> (a,b)
  in foldl' (||.) first rest

sc (Env (vref : _ ) _) (M.Literal dt n)
  = typedconst_ n vref dt

sc (Env (vref:_)_) (M.Identity {})
  = pos_ vref

sc env (M.Unary { M.unop=M.Year, M.arg }) =
  --assuming input is well formed and the column is an integer representing
  --a day count from 0000-01-01)
  -- using formula ((days+1)*1000+100)/365243 -- works for 1992 - 1997
  let dateval = sc env arg
      v365243 = const_ 365243 dateval -- we are approximating dates.  a more accurate formula requires one more digit and pushes us over the 32 bits.
  in (dateval*.(const_ 1000 dateval)) +. (const_ 1100 dateval) /. v365243 -- so we substract 1 instead, and  makes 1996-12-31 map to 1996 and 1997-01-01 to 1997.

--example use of isnull. In all the contexts of TPCH queries i saw, the isnull is called on
--a column or derived column that is statically known to not be null, so we just remove that.
{- sys.ifthenelse(sys.isnull(sys.=(all_nations.nation NOT NULL, char(25)[char(6) "BRAZIL"])), boolean "false", sys.=(all_nations.nation NOT NULL, char(25)[char(6) "BRAZIL"])) -}
sc env (M.IfThenElse { M.if_=M.Unary { M.unop=M.IsNull
                                     , M.arg=oper1 } , M.then_=M.Literal _  0, M.else_=oper2 }) | (oper1 == oper2) = sc env oper1 --just return the guarded operator.

-- note: for max and min, the actual possible bounds are more restrictive.
-- for now, I don't care.
sc env (M.IfThenElse { M.if_=mif_, M.then_=mthen_, M.else_=melse_ })=
  let if_ = sc env mif_
      then_ = sc env mthen_
      else_ = sc env melse_
  in  if_ ?.(then_,else_)

sc env (M.Like { M.ldata, M.lpattern }) =
  let sldata@Vexp {lineage} = sc env ldata
  in case lineage of
    Pure {col} -> complete $ Like { ldata=sldata, lpattern, lcol=col }
    None -> error "cannot apply like expressions without knowing lineage"

sc env (M.Unary { M.unop=M.Neg, M.arg }) =
  let slarg =  sc env arg
  in ones_ slarg -. slarg

sc _ e = error $ "unhandled mplan scalar expression: " ++ show e -- clean this up. requires non empty list

-- transformCast :: Vexp -> DType -> Vexp
-- transformCast vexp@Vexp{info=ColInfo{dtype=(DDecimal{point=cpoint},_)}}  dest@DDecimal{point=dpoint}
--   = let diff = dpoint - cpoint
--     in case signum diff of
--        0 -> vexp -- noop
--        -1 -> error "negative cast"
--        1 -> let mul@Vexp{info=mulinfo} = vexp *. (const_ (10^diff) vexp)
--             in mul{info=mulinfo{dtype=(dest,"decimal cast")}}
--        _ -> error "signum"
-- transformCast _ _ = error "implement this before calling"

solveAgg :: Config -> Env -> Env -> Vexp -> M.GroupAgg -> Vexp

solveAgg _ (Env [] _) _ _ _  = error "empty input env for group by"

-- average case dealt with by rewriting tp sum, count and finally using division.
solveAgg config env after gkeyvec (M.GAvg expr) =
  let gsums@Vexp { info=ColInfo { count=cgsums } } = solveAgg config env after gkeyvec (M.GFold M.FSum expr)
      gcounts@Vexp { info=ColInfo { bounds=(minc,_ ), count=cgcounts} } = solveAgg config env after gkeyvec M.GCount
  in assert (cgsums == cgcounts && minc == 1) gsums /. gcounts

-- count is rewritten to summing a one for every row
solveAgg config env after gkeyvec (M.GCount) =
  let rewrite = (M.GFold M.FSum (M.Literal DDecimal{point=0} 1))
  in solveAgg config env after gkeyvec rewrite

solveAgg config env (Env _ aftermap) gkeyvec g@(M.GFold op expr) =
  let foldop = case op of
        M.FSum -> FSum
        M.FMax -> FMax
        M.FMin -> FMin
        M.FChoose -> FChoose
      gdata = sc env expr
      defaultSolution = case format config of
        VdlFormat ->
          let (scattermask, sparsity) = getScatterMask config gkeyvec
              sortedGroups = gkeyvec `scatteredTo` scattermask
              sortedData = gdata `scatteredTo` scattermask
          in make2LevelFold sparsity config foldop sortedGroups sortedData
        VliteFormat -> let gmask = complete $ Semisort { sdata=gkeyvec }
                           fdata = gdata @@ gmask
                           fgroups = gkeyvec @@ gmask
                       in complete $ Fold {foldop, fdata, fgroups}
  in case g of
    (M.GFold M.FChoose (M.Ref nm)) ->
      case NameTable.lookup nm aftermap of
        Right (_, v) -> v -- this is already a grouped column, just return it instead of the aggregated version
        Left _ -> defaultSolution
    _ -> defaultSolution

data Sparsity = Sparse | Dense deriving (Show,Eq);


-- todo: rename this to 'isLargeGroupTable'
getSparsity :: (Integer,Integer) -> Integer -> Double -> Sparsity
getSparsity (minbound, maxbound) _ _ =
  let domain_size = (maxbound - minbound + 1)
  in if domain_size > 32000 then Sparse else Dense

-- scatter mask for a group by
getScatterMask :: Config -> Vexp -> (Vexp, Sparsity)
getScatterMask config predata@Vexp{ info=ColInfo {bounds=(pdatamin, pdatamax), count}}  =
   if pdatamax == pdatamin
   then (pos_ predata, Dense) -- pivots would be empty. unclear semantics.
   else let aggstrategy = aggregation_strategy config
            threshold = sparsity_threshold config
            pivots = complete $ RangeC { rmin=pdatamin
                                       , rstep=1
                                       , rcount=pdatamax-pdatamin+1
                                       }
            is_sparse = getSparsity (pdatamin,pdatamax) count threshold
            pdata = case (aggstrategy, is_sparse) of
              (AggSerial,_) -> predata
              (AggShuffle,_) -> complete $ VShuffle {varg=predata}
              (_,Sparse) -> complete $ VShuffle{varg=predata}
              _ -> predata
        in (complete $ Partition { pivots, pdata }, is_sparse)

maxForWidth :: Vexp -> Integer
maxForWidth vec =
  let width = getBitWidth vec
      -- examples:
      -- bitwidth is 0, then max should be 0:  (1 << 0) - 1 = (1 - 1) = 0
      -- bitwidth is 1, then max should be 0b1. (1 << 1) - 1 = (2 - 1) = 1 = 0xb1
      -- bitwidth is 2, then max should be 0xb11. (1 << 2) -1 = (4 - 1) = 3 = 0xb11
      -- cannot really subtract 1 from 1 << 31, b/c is underflow. so just check.
  in assert (width < 65) $ --"about to shift by 32 or more"
     (1 `shiftL` (fromInteger width)) - 1

addSizeHint :: Vexp -> Vexp -- returns an equivalnet vector with a bitmask hint for the voodoo backend
addSizeHint vec =
  let maxval = maxForWidth vec
      maxvalV = (const_ maxval vec){comment="size hint for voodoo backend"}
  in  vec &. maxvalV

addScatterSizeHint :: Vexp -> Vexp
addScatterSizeHint vec@Vexp{info=ColInfo {bounds=(vmin,vmax)}} =
  let maxvalV = assert (vmin >= 0) (const_ vmax vec){comment="scatter size hint for voodoo backend"}
  in  vec %. maxvalV


makeCompositeKey :: NonEmpty Vexp -> Reader Config Vexp
makeCompositeKey (firstvexp :| rest) =
  do offset <- asks gboffset
     oformat <- asks format
     let shifted = shiftToZero firstvexp -- needed bc empty list won't shift
     let out = foldl' composeKeys shifted rest
     let plusoffset@Vexp{info=originfo@ColInfo{bounds=(_,mx)}} =
           if offset > 0
           then (out +. const_ offset out){comment="offset added by goffset"}
           else out
     let hintFun = case oformat of
           VliteFormat -> \x -> x -- identity
           VdlFormat -> addSizeHint
     return $ hintFun $ plusoffset {info=originfo {bounds=(0,mx)}} -- override the lower bound.

--- makes the vector min be at 0 if it isnt yet.
shiftToZero :: Vexp -> Vexp
shiftToZero arg@Vexp { info=ColInfo { bounds=(vmin,_), trailing_zeros } }
  = if vmin == 0 && trailing_zeros == 0 then arg
    else let norm@Vexp { info=ColInfo {bounds=(vmin', _), trailing_zeros=t'} } = (arg >>. const_ trailing_zeros arg)
             ret@Vexp {info=ColInfo {bounds=(vmin'', _) }} = norm -. const_ vmin' norm
         in assert (vmin'' == 0 && t' == 0) ret

-- bitwidth required to represent all members
getBitWidth :: Vexp -> Integer
getBitWidth Vexp { info=ColInfo {bounds=(l,u)} }
  = max (bitsize l) (bitsize u)

bitsize :: Integer -> Integer
bitsize num =
  if num >= 0 then
    if num < toInteger (maxBound :: Int64) then
      let num64 = (fromInteger num) :: Int64
          ans = toInteger $ (finiteBitSize num64) - (countLeadingZeros num64)
      in assert (ans <= 64) ans
    else error (printf "number %d is larger than maxInt int64" num)
  else error (printf "bitwidth only allowed for non-negative numbers (num=%d)" num)


composeKeys :: Vexp -> Vexp -> Vexp
composeKeys l r =
  let sleft = shiftToZero l
      sright = shiftToZero r
      oldbits = getBitWidth sleft
      deltabits = getBitWidth sright
      newbits = oldbits + deltabits
  in assert (newbits < 65) $
     (sleft <<. (const_  deltabits sleft)) |. sright

-- Assumes the fgroups are alrady sorted
make2LevelFold :: Sparsity -> Config -> FoldOp -> Vexp -> Vexp -> Vexp
make2LevelFold sparsity config foldop fgroups fdata =
  let plain = Fold {foldop, fgroups, fdata} in
  case sparsity of
    Dense ->
      let ans = case aggregation_strategy config of
            AggSerial -> plain
            AggShuffle -> plain -- shuffle already done
            AggHierarchical gsz ->
              let pos = pos_ fgroups
                  log_gsize = const_ gsz fgroups
                  ones = ones_ fgroups
                  -- example: grainsize = 1. then log_gsize = 0. want 01010101... formula gives (pos >> 0) & 1 = 01010101...
                  -- example: grainsize = 2. then log_gisze = 1. want 00110011... formulate gives (pos >> 1) & 1 = 00110011
                  -- example: grainsize = 4. then log_gisze = 2. want 00001111... formulate gives (pos >> 2) & 1 ...
                  level1par = (pos >>. log_gsize) &. ones
                  level1groups = composeKeys fgroups level1par
                  level1results = complete $ Fold { foldop, fgroups=level1groups, fdata }
                  level2results = Fold { foldop, fgroups, fdata=level1results }
              in assert (getBitWidth level1par <= 1) level2results
      in complete $ ans
    Sparse -> complete $ plain -- shuffle already done


data JoinIdx = JoinIdx {selectmask::Vexp, gathermask::Vexp} deriving (Eq,Show)

handleGatherJoin :: Config -> Env -> Env -> M.JoinVariant -> JoinSpec -> [Vexp]
handleGatherJoin config (Env factcols _) (Env dimcols _) joinvariant jspec@(FKJoinSpec{joinorder=whichisleft}) =
 let JoinIdx {selectmask=selectboolean, gathermask} = deduceMasks config jspec
     selectmask = (complete $ Fold { foldop=FSel
                                  , fgroups=pos_ selectboolean
                                  , fdata= selectboolean
                                  }) {comment="selectmask"}
     (clean_gathermask, cleaned_factcols) = case gatherAll (gathermask:factcols) selectmask of
       a:b -> (a,b)
       _ -> error "unexpected empty"
     joined_dimcols = gatherAll dimcols clean_gathermask
 in case joinvariant of
   M.Plain ->  cleaned_factcols ++ joined_dimcols
   M.LeftSemi -> case whichisleft of -- semantics: left side
     FactDim -> cleaned_factcols
     DimFact -> let scattermask = gathermask{comment="dim semijoin fact scattermask"}
                    scatterFun = case format config of
                      VliteFormat -> scatteredTo
                      VdlFormat -> scatteredToWithHint
                    qualified = (ones_ scattermask) `scatterFun` scattermask
                    dimcolsselectmask = complete $ Fold { foldop=FSel
                                                        , fgroups=pos_ qualified
                                                        , fdata=qualified }
                in gatherAll dimcols dimcolsselectmask
   M.LeftOuter -> case whichisleft of
     FactDim -> error "left outer join on the fact side" -- this can be implemented
     DimFact -> error "left outer join on the dim side" -- looks a bit tougher to implement
   M.LeftAnti -> case whichisleft of
     FactDim -> let antiboolean = ones_ selectmask -. selectmask
                    antigather = complete $ Fold { foldop=FSel
                                                 , fgroups=pos_ antiboolean
                                                 , fdata=antiboolean}
                in gatherAll factcols antigather
     DimFact -> error "TODO implement anti for dimension table"

handleGatherJoin _ (Env leftcols _) (Env rightcols _) joinvariant  (SelfJoinSpec{leftmask, rightmask}) =
  let (factcols, dimcols, JoinIdx {gathermask}) =
        case (leftmask,rightmask) of -- only dealing with an easy case right now. can easily modify here for more invovled cases.
          (_,Vexp { vx=RangeV {rmin=0, rstep=1} }) -> (leftcols,rightcols, JoinIdx{gathermask=leftmask,
                                                                                   selectmask=ones_ leftmask})
          (Vexp { vx=RangeV {rmin=0, rstep=1} },_) -> (rightcols,leftcols, JoinIdx{gathermask=rightmask,
                                                                                   selectmask=ones_ rightmask}) -- use the idx1 as gather mask against cols2
          (_,_) -> error $ "TODO: handle case where both children of this self join have been modified (but one is unique)"
      cleaned_factcols = factcols -- no need to clean.
      joined_dimcols  = gatherAll dimcols gathermask
  in case joinvariant of
    M.Plain -> cleaned_factcols ++ joined_dimcols
    _ -> error $ "TODO: not a plain selfjoin: " ++ show joinvariant

deduceMasks :: Config -> JoinSpec -> JoinIdx
deduceMasks config (FKJoinSpec { factmask=fprime_fact_idx, factunique=quantf, dimmask=dimprime_dim_idx, joinidx=fact_dim_idx_name, dimref }) =
  let fact_dim_idx = let vx = Load fact_dim_idx_name
                     in Vexp { vx
                             , info = snd $ NameTable.lookup_err fact_dim_idx_name (colinfo config)
                             , lineage = None -- should this be none, or itself.
                             , name =  Nothing
                             , memoized_hash = sha1vx vx
                             , quant=Any
                             , comment=""
                             }
      -- fprime col tracks some fk col FK.
      -- FK is in one to one correspondance with the fact_dim_idx_name,
      -- so if we have a unique subset of FK, then it maps to a unique subset of fact_dim_idx_name
      -- so we use that information here, to label fprime_dim_idx accordingly.
      -- Query 18 makes this reasoning necessary.
      prelim_fprime_dim_idx = fact_dim_idx @@ fprime_fact_idx
      fprime_dim_idx = prelim_fprime_dim_idx {quant=quantf}
  in case dimprime_dim_idx of
    Vexp { quant=Unique } -> -- scattering back only works for columns with unique entries
      let ones = ones_ dimprime_dim_idx
          pos = pos_ dimprime_dim_idx
          dim_dimprime_valid = case format config of
            VdlFormat -> ones `scatteredTo` dimprime_dim_idx
            VliteFormat -> ones `tfScatterTo` (dimprime_dim_idx, dimref)
          dim_dimprime_idx = case format config of
            VdlFormat -> pos `scatteredTo` dimprime_dim_idx
            VliteFormat -> pos `tfScatterTo` (dimprime_dim_idx, dimref)
          fprime_dimprime_valid = dim_dimprime_valid @@ fprime_dim_idx
          fprime_dimprime_pos = dim_dimprime_idx @@ fprime_dim_idx
      in JoinIdx{selectmask=fprime_dimprime_valid, gathermask=fprime_dimprime_pos}

    Vexp {quant=Any} -> error $ "the dimension column is not known to be unique\n"

deduceMasks _ _  = error "not an fkjoinspec"

-- applies a gather to a group while preserving names
gatherAll :: [Vexp] -> Vexp -> [Vexp]
gatherAll cols shpos =
  do shsource <- cols
     return $ shsource @@ shpos

type Memoized = Map Vexp Vexp

xformIden :: [Vexp] -> [Vexp]
xformIden ins = xform (\x -> Just $ complete $ x) ins

redundantRangePattern :: Vx -> Maybe Vexp
redundantRangePattern (RangeV out1 out2 (Vexp{vx=(RangeV _ _ innerref)}))
  = Just $ complete $ RangeV out1 out2 innerref

redundantRangePattern _ = Nothing

algebraicIdentities :: Vx -> Maybe Vexp
algebraicIdentities (Binop {binop, left, right})
  | (binop == BitAnd || binop == BitOr) && left == right =
      Just left

algebraicIdentities (Binop {binop=BitAnd, left=zeros@Vexp{vx=RangeV{rmin=0,rstep=0}}})
  = Just zeros
algebraicIdentities (Binop {binop=BitAnd, right=zeros@Vexp{vx=RangeV{rmin=0,rstep=0}}})
  = Just zeros

algebraicIdentities (Binop {binop=BitOr, left=Vexp{vx=RangeV{rmin=0,rstep=0}}, right})
  = Just right

algebraicIdentities (Binop {binop=BitOr, left, right=Vexp{vx=RangeV{rmin=0,rstep=0}}})
  = Just left

algebraicIdentities (Binop {binop=BitShift, left=zeros@Vexp{vx=RangeV{rmin=0,rstep=0}}}) = Just zeros -- zeros stay constant
algebraicIdentities (Binop {binop=BitShift, left, right=Vexp{vx=RangeV{rmin=0,rstep=0}}}) = Just left -- noop


-- in the scatter case: the positions array must be exactly the size of the source at this point.
-- so this is really an identity if the scatter was legal in the first place.
algebraicIdentities (Shuffle {shop=Scatter, shpos=Vexp{vx=RangeV{rmin=0,rstep=1}}, shsource})
  = Just shsource

-- in the gather case, only if we know the size of the positions matches then this is an identity.
algebraicIdentities (Shuffle {shop=Gather, shpos=Vexp{vx=RangeV{rmin=0,rstep=1, rref}}, shsource})
  | rref==shsource = Just shsource

algebraicIdentities _ = Nothing

lowering :: Vx -> Maybe Vexp
lowering Binop{binop, left, right} =
  case binop of
    Max -> Just $ (left >. right) ?. (left, right)
    Min -> Just $ (left <. right) ?. (left, right)
    Neq -> Just $ (ones_ left) -. (left ==. right)
    _ -> Nothing

lowering _ = Nothing

loweringPass :: [Vexp] -> [Vexp]
loweringPass = xform lowering

algebraicIdentitiesPass :: [Vexp] -> [Vexp]
algebraicIdentitiesPass = xform algebraicIdentities

redundantRangePass :: [Vexp] -> [Vexp]
redundantRangePass = xform redundantRangePattern

xform :: (Vx -> Maybe Vexp) -> [Vexp] -> [Vexp]
xform fn vexps = -- preserve top level names
  let merge (accm,accl) vexp@Vexp {name}  = let (newm,newv) = transform fn vexp accm
                                            in (newm, newv{name=name}:accl)
      (_,outr) = foldl' merge (Map.empty, []) vexps
  in reverse outr

transform :: (Vx -> Maybe Vexp) -> Vexp -> Memoized -> (Memoized, Vexp)
transform fn vexp@(Vexp {vx, name, comment, info}) mp  =
  case Map.lookup vexp mp of
    Nothing ->
      let (mp', anon) = case vx of
            Load _ -> (mp, vexp) -- hack: we cannot deduce info for load without the config, so just keep it.
            _ ->  transformVx fn vx mp
          ans = anon {name=name, comment=comment, info=info} -- try to preserve names across optimizations
          mp'' = Map.insert vexp ans mp'
      in transform fn vexp mp'' -- should return. ensuring we are actually memozing.
    Just x@(Vexp {name=name'}) ->
      let new_name = case (name, name') of
            (Just a, _) -> Just a -- preserve it if it exists
            (Nothing, something) -> something
          newx = x {name=new_name}
          mp'' = Map.insert vexp newx mp
      in (mp'', newx)


transformVx :: (Vx -> Maybe Vexp) -> Vx -> Memoized -> (Memoized, Vexp)
transformVx fn vx mp =
  let (outmp, prelim) = case vx of
        CrossProduct {left,right,variant} ->
          let (mp', left') = transform fn left mp
              (mp'', right') = transform fn right mp'
          in (mp'', CrossProduct{left=left',right=right',variant})
        Load _ -> (mp, vx)
        RangeC {} -> (mp, vx)
        Semisort {sdata} ->
          let (mp', sdata') = transform fn sdata mp
          in (mp', Semisort {sdata=sdata'})
        RangeV a b rref ->
          let (mp',rref') = transform fn rref mp
          in (mp', RangeV a b rref')
        Binop a left right ->
          let (mp',left') = transform fn left mp
              (mp'', right') = transform fn right mp'
          in (mp'', Binop a left' right')
        Shuffle a shsource  shpos shshape ->
          let (mp',shsource') = transform fn shsource mp
              (mp'',shpos') = transform fn shpos mp'
          in (mp'', Shuffle a shsource' shpos' shshape)
        Fold a fgroups fdata ->
          let (mp',fgroups') = transform fn fgroups mp
              (mp'',fdata') = transform fn fdata mp'
          in (mp'', Fold a fgroups' fdata')
        Partition pivots pdata ->
          let (mp', pivots') = transform fn pivots mp
              (mp'',pdata') = transform fn pdata mp'
          in (mp'', Partition pivots' pdata')
        Like ldata a b ->
          let (mp', ldata') = transform fn ldata mp
          in (mp', Like ldata' a b)
        VShuffle varg ->
          let (mp', varg') = transform fn varg mp
          in (mp', VShuffle varg')
      xformPrelim = case fn prelim of -- now apply function to this.
        Nothing -> complete $ prelim -- pattern does not match anything.
        Just x -> x
  in (outmp, xformPrelim)


{-
Diagram to understand the join deduce masks code:

fact'                                     dim'
  |                                        |
  |  (fprime_fact_idx in lineage)          | (dimprime_dim_idx in lineage)
  |                                        |
  V                                        V
fact -----------------------------------> dim
         (fact_dim_idx provided by db)

for self joins:
eg. fact' join fact. => use fprime_fact_idx as gather positions.

 idx1  idx2
a---->b---->c
means we can use Gather {gatherpos=idx1, gatherdata=idx2}
to compute a direct index for a ---> c

   idx
a'------>a where a' is unique wrt. to a means we can do
Scatter {scatterpos=idx, scatterdata=ones} to get which entries of a remain in a'
Scatter {scatterpos=idx, scatterpos=pos } to get a gather revidx such that

   revidx
(filtered a valid) -----> a' is valid.

-}
