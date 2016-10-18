module Vdl (vdlFromVexps) where

import Types

import Control.Monad.Reader hiding (join)
import Control.Monad.State hiding (join)
import Data.Foldable(foldl')
import Config
import Name(Name(..), get_last, concat_name)
--import Data.Int
--import Debug.Trace
--import Text.Groom
import GHC.Generics
import Data.String.Utils(join, replace)
import Control.DeepSeq(NFData)
import qualified Vlite as V
import Data.Hashable
import qualified Data.HashMap.Strict as HMap
import qualified Data.ByteString.Lazy.Char8 as C

--import Data.List (foldl')
--import Config
import Prelude hiding (log)
--import qualified Error as E
import Sha

{-
Aims to cover the subset of VoodooStdLib we need for Monet plans.
Nothing in this type should be non-existant in Voodoo.
its function is to be easy to convert to text format.
-}
data Vd a =
  Load Name
  | Project { outname::Name , inname::Name, vec :: a } -- full rename only. used right after load
  | RangeV { rmin::Integer, rstep::Integer, rvec :: a }
  | RangeC {rmin::Integer, rstep::Integer, rcount::Integer }
  | Binary { op::Voodop, arg1::a, arg2::a  }
  | Scatter { scattersource::a, scatterfold::a, scatterpos::a }
  | Like { ldata::a, ldict::a, lpattern::C.ByteString }
  | VShuffle { varg::a }
  | MaterializeCompact { mout::a }
  deriving (Eq,Generic,Show)

-- instance Show (Vd a) where
--   show (MaterializeCompact {}) = "MaterializeCompact {}"
--   show (Load n) = "Load " ++ show n
--   show (RangeC{rmin,rstep,rcount}) = "RangeC { " ++ show rmin ++ ", " ++ show rstep ++ ", " ++ show rcount ++ " }"
--   show _ = error "showing non materialize"

instance (NFData a) => NFData (Vd a)
instance (Hashable a) => Hashable (Vd a)

-- int is memoized hash
data W = W (Voodoo, SHA1) deriving (Show,Generic)-- used so that it can recurse for the tree view
instance Hashable W where
  hashWithSalt s (W (_,b)) = hashWithSalt s (show b)

instance Eq W where
  (==) (W(_,h1)) (W(_,h2)) = h1 == h2
  -- use memoized hashes and hope for  the best

sha1vd :: Voodoo -> SHA1
sha1vd vd@(Load _, _) = sha1 $ C.pack $ show vd
sha1vd vd@(RangeC{},_) = sha1 $ C.pack $ show vd
sha1vd vd = sha1hack vd

completeW :: Voodoo -> W
completeW vd = W (vd, sha1vd vd)

--printable format for metadata
data Metadata = Metadata { databounds::(Integer,Integer)
                         , sizebound::Integer
                         , name::Maybe Name
                         , displaytype::DType
                         , origin::Maybe Name
                         , comment::C.ByteString}
  deriving (Eq,Show,Generic)
instance Hashable Metadata

getMetadata :: V.Vexp -> Metadata
getMetadata V.Vexp {V.info=ColInfo {bounds, count, dtype=(dt,notes)}, V.name, V.lineage, V.comment} =
  let origin = case lineage of
        V.Pure{ V.col } -> Just col
        V.None -> Nothing
  in Metadata {databounds=bounds,
               sizebound=count,
               name,
               displaytype=dt,
               origin,
               comment=C.intercalate " " [notes, comment]}

type VoodooMinus = Vd W
type Voodoo = (Vd W, Maybe Metadata)

newtype Id = Id Int
instance Show Id where
  show (Id n) = "Id " ++ show n

type Vref = Vd Id -- used for ref version that can be printed as a series of expressions


const_ :: Integer -> Voodoo -> Voodoo
const_ k v  = (RangeV { rmin=k, rstep=0, rvec=completeW v }, Nothing)

pos_ :: Voodoo -> Voodoo
pos_ v  = (RangeV { rmin=0, rstep=1, rvec=completeW v }, Nothing)

data Voodop =
  LogicalAnd
  | LogicalOr
  | BitwiseAnd
  | BitwiseOr
  | BitShift
  | Equals
  | Add
  | Subtract
  | Greater
  | Multiply
  | Divide
  | Modulo
  | FoldChoose
  | FoldSelect
  | FoldMax
  | FoldSum
  | FoldMin
  | FoldCount
  | Gather
  | Partition
  deriving (Eq,Show,Generic)
instance NFData Voodop
instance Hashable Voodop

-- convenience expression ops
(>.),(==.),(<.),(||.),(>>.),(&&.),(<=.),(>=.),(+.),(-.),(*.),(/.),(%.),(|.),(&.),(!=.) :: Voodoo -> Voodoo -> Voodoo
a >.  b = (Binary { op=Greater, arg1=completeW a, arg2=completeW b }, Nothing)
a ==. b = (Binary { op=Equals, arg1=completeW a, arg2=completeW b }, Nothing)
a <.  b = (Binary { op=Greater, arg1=completeW b, arg2=completeW a }, Nothing) --notice argument swap
a ||. b = (Binary { op=LogicalOr, arg1=completeW a, arg2=completeW b }, Nothing)
a >>. b = (Binary { op=BitShift, arg1=completeW a, arg2=completeW b }, Nothing)
a &&. b = (Binary { op=LogicalAnd, arg1=completeW a, arg2=completeW b }, Nothing)
a <=. b = (a <. b) ||. (a ==. b)
a >=. b = (a >. b) ||. (a ==. b)
a +. b =( Binary { op=Add, arg1=completeW a, arg2=completeW b }, Nothing)
a -. b =( Binary { op=Subtract, arg1=completeW a, arg2=completeW b }, Nothing)
a *. b = (Binary { op=Multiply, arg1=completeW a, arg2=completeW b }, Nothing)
a /. b = (Binary { op=Divide, arg1=completeW a, arg2=completeW b }, Nothing)
a %. b = (Binary { op=Modulo, arg1=completeW a, arg2=completeW b }, Nothing)
a |. b = (Binary { op=BitwiseOr, arg1=completeW a, arg2=completeW b }, Nothing)
a &. b = (Binary { op=BitwiseAnd, arg1=completeW a, arg2=completeW b }, Nothing)
a !=. b = (const_ 1 a) -. (a ==. b) -- NOTE: equals check needed to make sure we
-- only have 0 or 1 in the multiplication.

(?.) :: Voodoo -> (Voodoo,Voodoo) -> Voodoo
cond ?. (a,b) = ((const_ 1 a  -. negcond) *. a) +. (negcond *. b)
  where negcond = (cond ==. const_ 0 a)

type MemoTable = HMap.HashMap V.Vexp Voodoo

makeload :: Name -> VoodooMinus
makeload n =
  let inname = (case n of
                   Name (_:s:rest) -> Name $ s:rest
                   _ -> error  "need longer keypath to be consistent with ./Driver keypaths")
  in Project { outname=Name ["val"]
             , inname
             , vec = completeW $ (Load n, Nothing) }


voodooFromVexpMemo :: V.Vexp -> State MemoTable Voodoo
voodooFromVexpMemo vexp@(V.Vexp vx _ _ _ _ _ _) =
  do s <- get -- used to access the state for a lookup.
     case HMap.lookup vexp s of -- must be of type State MemoTable Voodoo. but the state must be updated.
       Nothing -> do r <- voodooFromVxNoMemo vx -- State MemoTable VoodooMinus
                     let completed = (r, Just $ getMetadata vexp)
                     modify $ HMap.insert vexp completed -- type is MemoTable -> MemoTable
                     return completed
       Just ans -> return ans

voodooFromVxNoMemo :: V.Vx -> State MemoTable VoodooMinus

voodooFromVxNoMemo (V.Load n) = return $ makeload n

voodooFromVxNoMemo (V.RangeV {V.rmin, V.rstep, V.rref}) =
  do v <- voodooFromVexpMemo rref -- State MemoTable Voodo
     return $ RangeV {rmin, rstep, rvec=completeW v}


-- 1. removed state from the signature (only shows up once, can even give an alias )
-- 2. removed also from arguments, no need to mention it.
-- 3. case where it is not changed: return of thing that does not mention it. (implicitly returns the one we had)
-- 4. case where it must be changed because of a call to another function that returns a State MemoTable X:
       -- do x <- myFun.  If the function implicitly takes in the current state (eg because it is a state action in itself)
       --    return $ f x -- wont it
voodooFromVxNoMemo (V.RangeC {V.rmin, V.rstep, V.rcount}) =
  return RangeC {rmin, rstep, rcount }

voodooFromVxNoMemo (V.Binop { V.binop, V.left, V.right}) =
  do l <- voodooFromVexpMemo left
     r <- voodooFromVexpMemo right
     let (t,_) = case binop of
           V.Gt -> l >. r
           V.Eq -> l ==. r
           V.Mul -> l *. r
           V.Sub -> l -. r
           V.Add -> l +. r
           V.Lt -> l <. r
           V.Leq -> l <=. r
           V.Geq -> l >=. r
           V.Min -> (l <=. r) ?. (l, r)
           V.Max -> (l >=. r) ?. (l, r)
           V.LogAnd -> (l &&. r)
           V.LogOr -> (l ||. r)
           V.Div -> (l /. r)
           V.BitShift ->  (l >>. r)
           V.Neq ->  (l !=. r)
           V.BitOr -> (l |. r)
           V.BitAnd -> (l &. r)
           V.Mod -> (l %. r)
     return t


voodooFromVxNoMemo (V.Shuffle { V.shop,  V.shsource, V.shpos }) =
  do source <- voodooFromVexpMemo shsource
     positions <- voodooFromVexpMemo shpos
     return $ case shop of
       V.Gather -> Binary { op=Gather, arg1=completeW source, arg2=completeW positions }
       V.Scatter -> let scatterfold = case source of
                          (RangeV {rmin=0,rstep=1},_) -> completeW source -- avoid duplicating ranges, or else it looks like passes do not work.
                          _ -> completeW $ pos_ source
                    in Scatter { scattersource=completeW source, scatterfold, scatterpos=completeW positions }

voodooFromVxNoMemo (V.Like { V.ldata, V.lpattern, V.lcol }) =
  do newldata <- voodooFromVexpMemo ldata
     let ldict = let Name ns = lcol in makeload $ Name (ns ++ ["heap"])
     return $ Like {ldata=completeW newldata, ldict=completeW (ldict, Nothing), lpattern}

voodooFromVxNoMemo (V.Like { }) = error "like needs a lineage for the dictionary"

voodooFromVxNoMemo (V.VShuffle { V.varg }) =
  do newarg <- voodooFromVexpMemo varg
     return $ VShuffle $ completeW newarg

voodooFromVxNoMemo (V.Fold { V.foldop, V.fgroups, V.fdata}) =
  do arg1 <- voodooFromVexpMemo fgroups
     arg2 <- voodooFromVexpMemo fdata
     let op = case foldop of
           V.FChoose -> FoldChoose
           V.FSum -> FoldSum
           V.FMax -> FoldMax
           V.FMin -> FoldMin
           V.FSel -> FoldSelect
     return $ Binary { op, arg1=completeW arg1, arg2=completeW arg2 }

voodooFromVxNoMemo (V.Partition {V.pdata, V.pivots}) =
  do arg1 <- voodooFromVexpMemo pdata
     arg2 <- voodooFromVexpMemo pivots
     return $ Binary {op=Partition, arg1=completeW arg1, arg2=completeW arg2 }

voodoosFromVexps :: [V.Vexp] -> Config -> [Voodoo]
voodoosFromVexps vexps config =
  -- traceShow vexps $
  let solve (s, res) v =  let meta = Just $ getMetadata v -- preserve output metadata
                              ((v',_),s') = runState (voodooFromVexpMemo v) s
                          in  (s', (v',meta):res)
      (_, ans)  = foldl' solve (HMap.empty,[]) vexps
      rename_value vec@(_, meta@(Just (Metadata { name, origin }))) =
        case format config of
          VliteFormat -> vec
          VdlFormat ->
            let catted = case (name,origin) of
                  (Just n, Just y) -> (concat_name (get_last n) y)
                  (Just n, Nothing) -> get_last n
                  (Nothing, Just y) -> (concat_name (Name ["val"]) y)
                  (_,_) -> Name ["val"]
                disp = show catted
                newname = replace ("."::String) "__" disp
                outname = Name [C.pack newname]
            in (Project { outname, inname=Name ["val"], vec=completeW vec }, fmap (\m -> m {comment="rename for output"}) meta)
      rename_value vec@(_, _) = vec -- in case not found
  in map (\r@(_,m) -> ((MaterializeCompact .  completeW . rename_value) r, m)) ans

vrefsFromVoodoos :: [Voodoo] -> Log
vrefsFromVoodoos vecs =
  let action = sequence $ map (\v -> memVrefFromVoodoo v) vecs
      state0 = (Id 0,HMap.empty,[])
      (_,_,log) = execState action state0
  in reverse log

type Log = [(Id, Vref, Maybe Metadata)] -- a numbered list of expressions using numbers to refer to previous ones
type SStat = (Id, HMap.HashMap Voodoo Id, Log) -- int is the 'last log number' used. or 0.

updateState :: Voodoo -> (Vref, Maybe Metadata) -> State SStat Id
updateState vd (vref,info) =
  do (Id n,tab,log) <- get
     let id' = Id $ n+1
     let tab' = HMap.insert vd id' tab
     let log' = (id', vref, info) : log
     put (id', tab', log')
     return id'

--used to remember common expressions from before
memVrefFromVoodoo :: Voodoo -> State SStat Id
memVrefFromVoodoo vd@(vdminus, info) =
  do (_,tab,_) <- get -- note there isn't even a name to refer to the state (other than get..)
     case HMap.lookup vd tab of
       Nothing -> do vref <- vrefFromVoodoo vdminus
                     updateState vd (vref, info)
       Just iden -> return iden

vrefFromVoodoo :: VoodooMinus -> State SStat Vref

vrefFromVoodoo (Load n) = return (Load n)

vrefFromVoodoo (RangeC {rmin,rstep,rcount}) = return $ RangeC {rmin,rstep,rcount}

vrefFromVoodoo (RangeV  {rmin,rstep,rvec=W (rvec,_)}) =
  do rvec <- memVrefFromVoodoo rvec
     return $ RangeV { rmin,rstep, rvec }

vrefFromVoodoo (Scatter {scattersource=W (scattersource,_)
                              , scatterfold=W (scatterfold,_)
                              , scatterpos=W (scatterpos,_) }) =
  do scattersource <- memVrefFromVoodoo scattersource
     scatterfold <- memVrefFromVoodoo scatterfold
     scatterpos <- memVrefFromVoodoo scatterpos
     return $ Scatter {scattersource, scatterfold, scatterpos}

vrefFromVoodoo (Project {outname, inname, vec=W (vec,_)}) =
  do vec <- memVrefFromVoodoo vec
     return $ Project { outname, inname, vec }

vrefFromVoodoo (Binary { op, arg1=W (arg1,_) , arg2=W (arg2,_) }) =
  do arg1 <-  memVrefFromVoodoo arg1
     arg2 <-  memVrefFromVoodoo arg2
     return $ Binary {op, arg1, arg2}

vrefFromVoodoo (Like { ldata=W (ldata,_), ldict=W (ldict,_),  lpattern}) =
  do ldata <- memVrefFromVoodoo ldata
     ldict <- memVrefFromVoodoo ldict
     return $ Like { ldata, ldict, lpattern }

vrefFromVoodoo (VShuffle { varg=W(varg,_)}) =
  do varg <- memVrefFromVoodoo varg
     return $ VShuffle {varg}

vrefFromVoodoo (MaterializeCompact {mout=W (mout,_)}) =
  do mout <- memVrefFromVoodoo mout
     return $ MaterializeCompact mout

toVList :: Vref -> [String]
-- for printing: remove sys (really, we want the prefix only_
toVList (Load n) = ["Load", show n]
toVList (Project {vec}) = ["Project",show vec]
toVList (RangeV { rmin, rstep, rvec }) = ["RangeV", show rmin, show rvec, show rstep]
toVList (RangeC { rmin, rstep, rcount }) = ["RangeC", show rmin, show rcount, show rstep]
toVList (Binary { op, arg1, arg2 }) =
  case op of
    Gather -> [sop, id1, id2]
    _ -> [sop, id1, id2]
  where sop = show op
        id1 = show arg1
        id2 = show arg2

toVList (Scatter { scattersource, scatterfold, scatterpos } ) =
  ["Scatter", id1, id2, id3]
  where id1 = show scattersource
        id2 = show scatterfold
        id3 = show scatterpos

toVList (Like { ldata, ldict, lpattern } ) =
  ["Like", id1, id2, C.unpack lpattern]
  where id1 = show ldata
        id2 = show ldict

toVList (VShuffle {varg}) =
  ["Shuffle", show varg]

toVList (MaterializeCompact x) =
  ["Output", show x]

toVList _ = error "implement me"


{- now a list of strings -}
toVoodooList :: Vref -> [String]

-- for printing: remove sys (really, we want the prefix only_
toVoodooList (Load (Name lst)) = ["Load", show cleanname]
  where cleanname = Name (if head lst == "sys" then tail lst else lst)

toVoodooList (Project {outname, inname, vec}) =
  ["Project",show outname, show vec, show inname ]

toVoodooList (RangeV { rmin, rstep, rvec }) =
  {- for printing: hardcoded length 2 billion right now,
  since backend only materializes what's needed -}
  ["RangeV", "val", show rmin, show rvec, show rstep]

toVoodooList (RangeC { rmin, rstep, rcount }) =
  ["RangeC", "val", show rmin, show rcount, show rstep]

toVoodooList (Binary { op, arg1, arg2}) =
  case op of
    Gather -> [show op, show arg1, show arg2, "val"]
    _ ->      [show op, "val", show arg1, "val", show arg2, "val" ]

toVoodooList (Scatter { scattersource, scatterfold, scatterpos } ) =
  ["Scatter", show scattersource, show scatterfold, "val", show scatterpos, "val"]

toVoodooList (Like { ldata, ldict, lpattern } ) =
  ["Like", "val",id1, "val",id2,"val", C.unpack lpattern]
  where id1 = show ldata
        id2 = show ldict

toVoodooList (VShuffle {varg}) =
  ["Shuffle", show varg]

toVoodooList (MaterializeCompact x) =
  ["MaterializeCompact",show x]

printLine :: (Id, Vref, Maybe Metadata) -> Reader Config String
printLine (Id iden, vref, info) =
  do display <- asks show_metadata -- Reader Config Boolean. actually need to use the boolean here.
     format <- asks format
     let printer = case format of
           VdlFormat -> toVoodooList
           VliteFormat -> toVList
     let strs = printer vref
     let dispinfo = case info of
           Nothing -> ""
           Just x -> " ;; " ++ show x
     let meta = if display then dispinfo else ""
     let fstrs = case (format, vref, info) of
           (VliteFormat, (MaterializeCompact _), (Just (Metadata { name=Just n, displaytype }))) ->
             let typstring = case displaytype of
                   DDecimal {point} -> "decimal_" ++ show point
                   DString {decoder} -> "string_" ++ show decoder
                   DDate -> "date"
                 prettyN = case n of
                   Name alist -> last alist
             in [C.unpack prettyN, "Output", typstring] ++ tail strs  --
           _ -> [show iden] ++ strs
     return $ (join "," $ fstrs) ++ meta

dumpVref :: Log -> Reader Config String
dumpVref tuples = let lns = mapM printLine tuples
                  in  join "\n" <$> lns


-- forces the printer to use our custom format rather than
-- a default
data Vdl = Vdl String
instance Show Vdl where
  show (Vdl s) = s

vdlFromVexps :: [V.Vexp] -> Reader Config Vdl
vdlFromVexps vexps =
  do conf <- ask
     let voodoos = voodoosFromVexps vexps conf
     let log  = vrefsFromVoodoos voodoos
     Vdl <$> dumpVref log
