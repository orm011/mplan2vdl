module Vdl (vdlFromVexps) where

import Control.Monad.Reader hiding (join)
import Data.Foldable(foldl')
import Config
import Name(Name(..), get_last, concat_name)
--import Data.Int
import Debug.Trace
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
  deriving (Eq,Show,Generic)
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
                         , origin::Maybe Name
                         , comment::C.ByteString}
  deriving (Eq,Show,Generic)
instance Hashable Metadata

getMetadata :: V.Vexp -> Metadata
getMetadata V.Vexp {V.info=ColInfo {bounds, count}, V.name, V.lineage, V.comment} =
  let origin = case lineage of
        V.Pure{ V.col } -> Just col
        V.None -> Nothing
  in Metadata {databounds=bounds, sizebound=count, name, origin, comment}

type VoodooMinus = Vd W
type Voodoo = (Vd W, Maybe Metadata)

type Vref = Vd Int -- used for ref version that can be printed as a series of expressions

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

-- (.^.) :: (Int,Int) -> (Int,Int) -> (Int,Int)
-- (a,b) .^. (a',b') = (max a a', b + b')

-- size :: Voodoo -> (Int, Int)
-- size (Load _) = (1,1)
-- size (Scatter (W ch1) (V ch2) (V ch3)) = let (a,b) = (size ch1) .^. (size ch2) .^. (size ch3) in (a + 1, b + 1)
-- size (RangeV _ _ _) = (1,1)
-- size (RangeC _ _ _) = (1,1)
-- size (Project _ _ (V ch)) = let (a,b) = (size ch) .^. (1,1) in (a+1, b+1)
-- size (Binary _ (V ch1) (V ch2)) = let (a,b) = (size ch1) .^. (size ch2) in (a+1,b+1)

-- dagSize :: [Voodoo] -> (Int,Int)
-- dagSize outputs = let (a,b) = foldl' (.^.) (0,0) (map size outputs) in (a+1,b+(length outputs))

-- convenience expression library to translate more complex
-- all have type Voodoo -> Voodoo -> Voodoo
(>.) :: Voodoo -> Voodoo -> Voodoo
a >.  b = (Binary { op=Greater, arg1=completeW a, arg2=completeW b }, Nothing)

(==.) :: Voodoo -> Voodoo -> Voodoo
a ==. b = (Binary { op=Equals, arg1=completeW a, arg2=completeW b }, Nothing)

(<.) :: Voodoo -> Voodoo -> Voodoo
a <.  b = (Binary { op=Greater, arg1=completeW b, arg2=completeW a }, Nothing) --notice argument swap

(||.) :: Voodoo -> Voodoo -> Voodoo
a ||. b = (Binary { op=LogicalOr, arg1=completeW a, arg2=completeW b }, Nothing)

(>>.) :: Voodoo -> Voodoo -> Voodoo
a >>. b = (Binary { op=BitShift, arg1=completeW a, arg2=completeW b }, Nothing)

(&&.) :: Voodoo -> Voodoo -> Voodoo
a &&. b = (Binary { op=LogicalAnd, arg1=completeW a, arg2=completeW b }, Nothing)

(<=.) :: Voodoo -> Voodoo -> Voodoo
a <=. b = (a <. b) ||. (a ==. b)

(>=.) :: Voodoo -> Voodoo -> Voodoo
a >=. b = (a >. b) ||. (a ==. b)

(+.) :: Voodoo -> Voodoo -> Voodoo
a +. b =( Binary { op=Add, arg1=completeW a, arg2=completeW b }, Nothing)

(-.) :: Voodoo -> Voodoo -> Voodoo
a -. b =( Binary { op=Subtract, arg1=completeW a, arg2=completeW b }, Nothing)

(*.) :: Voodoo -> Voodoo -> Voodoo
a *. b = (Binary { op=Multiply, arg1=completeW a, arg2=completeW b }, Nothing)

(/.) :: Voodoo -> Voodoo -> Voodoo
a /. b = (Binary { op=Divide, arg1=completeW a, arg2=completeW b }, Nothing)

(%.) :: Voodoo -> Voodoo -> Voodoo
a %. b = (Binary { op=Modulo, arg1=completeW a, arg2=completeW b }, Nothing)

(|.) :: Voodoo -> Voodoo -> Voodoo
a |. b = (Binary { op=BitwiseOr, arg1=completeW a, arg2=completeW b }, Nothing)

(&.) :: Voodoo -> Voodoo -> Voodoo
a &. b = (Binary { op=BitwiseAnd, arg1=completeW a, arg2=completeW b }, Nothing)

(?.) :: Voodoo -> (Voodoo,Voodoo) -> Voodoo
cond ?. (a,b) = ((const_ 1 a  -. negcond) *. a) +. (negcond *. b)
  where negcond = (cond ==. const_ 0 a)

(!=.) :: Voodoo -> Voodoo -> Voodoo
a !=. b = (const_ 1 a) -. (a ==. b)
-- NOTE: check needed to make sure we
-- only have 0 or 1 in the multiplication.

type MemoTable = HMap.HashMap V.Vexp Voodoo

makeload :: Name -> VoodooMinus
makeload n =
  let inname = (case n of
                   Name (_:s:rest) -> Name $ s:rest
                   _ -> error  "need longer keypath to be consistent with ./Driver keypaths")
  in Project { outname=Name ["val"]
             , inname
             , vec = completeW $ (Load n, Nothing) }

voodooFromVexpMemo :: MemoTable -> V.Vexp -> (MemoTable, Voodoo)
voodooFromVexpMemo s vexp@(V.Vexp vx _ _ _ _ _ _) =
  case HMap.lookup vexp s of
    Nothing -> let (s',r) = voodooFromVxNoMemo s vx
                   ans = (r, Just $ getMetadata vexp)
                   s'' = HMap.insert vexp ans s'
               in  (s'', ans)
    Just ans -> (s, ans)

voodooFromVxNoMemo :: MemoTable -> V.Vx -> (MemoTable, VoodooMinus)

voodooFromVxNoMemo st (V.Load n) = (st, makeload n)

voodooFromVxNoMemo s (V.RangeV {V.rmin, V.rstep, V.rref}) =
  let (s',v) = voodooFromVexpMemo s rref
  in (s', RangeV {rmin, rstep, rvec=completeW v})

voodooFromVxNoMemo s (V.RangeC {V.rmin, V.rstep, V.rcount}) =
  (s, RangeC {rmin, rstep, rcount })

voodooFromVxNoMemo s (V.Binop { V.binop, V.left, V.right}) =
  let (s', l) = voodooFromVexpMemo s left
      (s'', r) = voodooFromVexpMemo s' right
      (t,_) = case binop of
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
  in (s'',t)

voodooFromVxNoMemo s (V.Shuffle { V.shop,  V.shsource, V.shpos }) =
  let (s', source) = voodooFromVexpMemo s shsource
      (s'', positions) = voodooFromVexpMemo s' shpos
      scatter = case shop of
        V.Gather -> Binary { op=Gather, arg1=completeW source, arg2=completeW positions }
        V.Scatter -> let scatterfold = case source of
                           (RangeV {rmin=0,rstep=1},_) -> completeW source -- avoid duplicating ranges, or else it looks like passes do not work.
                           _ -> completeW $ pos_ source
                     in Scatter { scattersource=completeW source, scatterfold, scatterpos=completeW positions }
  in (s'', scatter)

voodooFromVxNoMemo s (V.Like { V.ldata, V.lpattern, V.lcol }) =
  let (s', newldata)  = voodooFromVexpMemo s ldata
      ldict = let Name ns = lcol in makeload $ Name (ns ++ ["heap"])
  in (s', Like {ldata=completeW newldata, ldict=completeW (ldict, Nothing), lpattern})

voodooFromVxNoMemo _ (V.Like { }) = error "like needs a lineage for the dictionary"

voodooFromVxNoMemo s (V.VShuffle { V.varg }) =
  let (s', newarg) = voodooFromVexpMemo s varg
  in (s', VShuffle $ completeW newarg)

voodooFromVxNoMemo s (V.Fold { V.foldop, V.fgroups, V.fdata}) =
  let (s', arg1) = voodooFromVexpMemo s fgroups
      (s'', arg2) = voodooFromVexpMemo s' fdata
      op = case foldop of
        V.FSum -> FoldSum
        V.FMax -> FoldMax
        V.FMin -> FoldMin
        V.FSel -> FoldSelect
  in (s'', Binary { op, arg1=completeW arg1, arg2=completeW arg2 })

voodooFromVxNoMemo s (V.Partition {V.pdata, V.pivots}) =
  let (s', arg1) = voodooFromVexpMemo s pdata
      (s'', arg2) = voodooFromVexpMemo s' pivots
  in (s'', Binary {op=Partition, arg1=completeW arg1, arg2=completeW arg2 })

voodoosFromVexps :: [V.Vexp] -> [Voodoo]
voodoosFromVexps vexps =
  let solve (s, res) v =  let (s', v') = voodooFromVexpMemo s v
                          in  (s', v':res)
      (_, ans)  = foldl' solve (HMap.empty,[]) vexps
      rename_value vec@(_, meta@(Just (Metadata { name, origin }))) =
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
  in map (\r -> ((MaterializeCompact .  completeW . rename_value) r, Nothing)) ans

vrefsFromVoodoos :: [Voodoo] -> Log
vrefsFromVoodoos vecs =
  let log0 = [(0, Load $ Name ["dummy"], Nothing)]
      state0 = ((HMap.empty, log0), undefined)
      ((_, finalLog),_) = foldl' process state0 vecs
      post = tail $ reverse $ finalLog -- remove dummy, reverse
     --let tr = "\n--Vref output:\n" ++ groom post
     --return $ trace tr post
  in trace (show $ length post) post
  where process (state, _) v  = memVrefFromVoodoo state v

type LookupTable = HMap.HashMap Voodoo Int
type Log = [(Int, Vref, Maybe Metadata)]
type State = (LookupTable, Log)

addToLog :: Log -> (Vref, Maybe Metadata) -> (Log, Int)
addToLog log (v,info) = let (n, _, _) = head log
                 in ((n+1, v, info) : log, n+1)

--used to remember common expressions from before
memVrefFromVoodoo :: State -> Voodoo -> (State, Int)
memVrefFromVoodoo state@(tab, log) vd@(vdminus, info) =
  case HMap.lookup vd tab of
    Nothing -> let ((tab', log'), vref) = vrefFromVoodoo state vdminus
                   (log'', iden) = addToLog log' (vref, info)
                   tab'' = HMap.insert vd iden tab'
               in ((tab'', log''), iden)
    Just iden -> ((tab, log), iden) -- nothing changes

vrefFromVoodoo :: State -> VoodooMinus -> (State, Vref)

vrefFromVoodoo state (Load n) = (state, Load n)

vrefFromVoodoo state (RangeC {rmin,rstep,rcount} ) =
  (state, RangeC {rmin,rstep,rcount})

vrefFromVoodoo state (RangeV  {rmin,rstep,rvec=W (rvec,_)}) =
  let (state', n') = memVrefFromVoodoo state rvec
  in (state', RangeV { rmin,rstep, rvec= n' })

vrefFromVoodoo state (Scatter {scattersource=W (scattersource,_)
                              , scatterfold=W (scatterfold,_)
                              , scatterpos=W (scatterpos,_) }) =
  let (state', n') = memVrefFromVoodoo state scattersource
      (state'', n'') = memVrefFromVoodoo state' scatterfold
      (state''', n''') = memVrefFromVoodoo state'' scatterpos
  in (state''', Scatter {scattersource= n', scatterfold= n'', scatterpos= n'''})

vrefFromVoodoo state (Project {outname, inname, vec=W (vec,_)}) =
  let (state', n') = memVrefFromVoodoo state vec
  in (state', Project { outname, inname, vec=n' })


vrefFromVoodoo state (Binary { op, arg1=W (arg1,_) , arg2=W (arg2,_) }) =
  let (state', n1) = memVrefFromVoodoo state arg1
      (state'', n2) = memVrefFromVoodoo state' arg2
  in  (state'', Binary { op, arg1=n1, arg2=n2})

vrefFromVoodoo state (Like { ldata=W (ldata,_), ldict=W (ldict,_),  lpattern}) =
  let (state', n1) = memVrefFromVoodoo state ldata
      (state'', n2) = memVrefFromVoodoo state' ldict
  in (state'', Like { ldata=n1, ldict=n2, lpattern})

vrefFromVoodoo state (VShuffle { varg=W(varg,_)}) =
  let  (state', n1) = memVrefFromVoodoo state varg
  in (state', VShuffle {varg=n1})

vrefFromVoodoo state (MaterializeCompact {mout=W (mout,_)}) =
  let (state', n') = memVrefFromVoodoo state mout
  in (state', MaterializeCompact n' )

{- now a list of strings -}
toList :: Vref -> [String]

-- for printing: remove sys (really, we want the prefix only_
toList (Load (Name lst)) = ["Load", show cleanname]
  where cleanname = Name (if head lst == "sys" then tail lst else lst)

toList (Project {outname, inname, vec}) =
  ["Project",show outname, "Id " ++ show vec, show inname ]

toList (RangeV { rmin, rstep, rvec }) =
  {- for printing: hardcoded length 2 billion right now,
  since backend only materializes what's needed -}
  ["RangeV", "val", show rmin, "Id " ++ show rvec, show rstep]

toList (RangeC { rmin, rstep, rcount }) =
  ["RangeC", "val", show rmin, show rcount, show rstep]

toList (Binary { op, arg1, arg2}) =
  case op of
    Gather -> [sop, id1, id2, "val"]
    _ ->      [sop, "val", id1, "val", id2, "val" ]
  where sop = show op
        id1 = "Id " ++ show arg1
        id2 = "Id " ++ show arg2

toList (Scatter { scattersource, scatterfold, scatterpos } ) =
  ["Scatter", id1, id2, "val", id3, "val"]
  where id1 = "Id " ++ show scattersource
        id2 = "Id " ++ show scatterfold
        id3 = "Id " ++ show scatterpos

toList (Like { ldata, ldict, lpattern } ) =
  ["Like", "val",id1, "val",id2,"val", C.unpack lpattern]
  where id1 = "Id " ++ show ldata
        id2 = "Id " ++ show ldict

toList (VShuffle {varg}) =
  ["Shuffle", "Id " ++ show varg]

toList (MaterializeCompact x) =
  ["MaterializeCompact","Id " ++ show x]

printLine :: (Int, Vref, Maybe Metadata) -> Reader Config String
printLine (iden, vref, info) =
  do display <- asks show_metadata
     let strs = toList vref
     let dispinfo = case info of
           Nothing -> ""
           Just x -> "  ;; " ++ show x
     let meta = if display then dispinfo else ""
     return $ (join "," $ (show iden) : strs) ++ meta

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
  let voodoos = voodoosFromVexps vexps
      log  = vrefsFromVoodoos voodoos
  in Vdl <$> dumpVref log
