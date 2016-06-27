module Vdl (vdlFromVexps) where

import Control.Monad(foldM)
import Name(Name(..))
--import Data.Int
import Debug.Trace
--import Text.Groom
import GHC.Generics
import Data.String.Utils(join)
import Control.DeepSeq(NFData)
import qualified Vlite as V
import Data.Hashable
import qualified Data.HashMap.Strict as HMap
import qualified Data.ByteString.Lazy.Char8 as C
--import Data.List (foldl')
import Config
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
  deriving (Eq,Show,Generic)
instance (NFData a) => NFData (Vd a)
instance (Hashable a) => Hashable (Vd a)

-- int is memoized hash
data W = W (Vd W, SHA1) deriving (Show,Generic)-- used so that it can recurse for the tree view
instance Hashable W where
  hashWithSalt s (W (_,b)) = hashWithSalt s (show b)

instance Eq W where
  (==) (W(_,h1)) (W(_,h2)) = h1 == h2
  -- use memoized hashes and hope for  the best

sha1vd :: Voodoo -> SHA1
sha1vd vd@(Load _) = sha1 $ C.pack $ show vd
sha1vd vd@RangeC{} = sha1 $ C.pack $ show vd
sha1vd vd = sha1hack vd

completeW :: Voodoo -> W
completeW vd = W (vd, sha1vd vd)

type Voodoo = Vd W

type Vref = Vd Int -- used for ref version that can be printed as a series of expressions

const_ :: Integer -> Voodoo -> Voodoo
const_ k v  = RangeV { rmin=k, rstep=0, rvec=completeW v }

pos_ :: Voodoo -> Voodoo
pos_ v  = RangeV { rmin=0, rstep=1, rvec=completeW v }

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
a >.  b = Binary { op=Greater, arg1=completeW a, arg2=completeW b }

(==.) :: Voodoo -> Voodoo -> Voodoo
a ==. b = Binary { op=Equals, arg1=completeW a, arg2=completeW b }

(<.) :: Voodoo -> Voodoo -> Voodoo
a <.  b = Binary { op=Greater, arg1=completeW b, arg2=completeW a } --notice argument swap

(||.) :: Voodoo -> Voodoo -> Voodoo
a ||. b = Binary { op=LogicalOr, arg1=completeW a, arg2=completeW b }

(>>.) :: Voodoo -> Voodoo -> Voodoo
a >>. b = Binary { op=BitShift, arg1=completeW a, arg2=completeW b }

(&&.) :: Voodoo -> Voodoo -> Voodoo
a &&. b = Binary { op=LogicalAnd, arg1=completeW a, arg2=completeW b }

(<=.) :: Voodoo -> Voodoo -> Voodoo
a <=. b = (a <. b) ||. (a ==. b)

(>=.) :: Voodoo -> Voodoo -> Voodoo
a >=. b = (a >. b) ||. (a ==. b)

(+.) :: Voodoo -> Voodoo -> Voodoo
a +. b = Binary { op=Add, arg1=completeW a, arg2=completeW b }

(-.) :: Voodoo -> Voodoo -> Voodoo
a -. b = Binary { op=Subtract, arg1=completeW a, arg2=completeW b }

(*.) :: Voodoo -> Voodoo -> Voodoo
a *. b = Binary { op=Multiply, arg1=completeW a, arg2=completeW b }

(/.) :: Voodoo -> Voodoo -> Voodoo
a /. b = Binary { op=Divide, arg1=completeW a, arg2=completeW b }

(|.) :: Voodoo -> Voodoo -> Voodoo
a |. b = Binary { op=BitwiseOr, arg1=completeW a, arg2=completeW b }

(&.) :: Voodoo -> Voodoo -> Voodoo
a &. b = Binary { op=BitwiseAnd, arg1=completeW a, arg2=completeW b }

(?.) :: Voodoo -> (Voodoo,Voodoo) -> Voodoo
cond ?. (a,b) = ((const_ 1 a  -. negcond) *. a) +. (negcond *. b)
  where negcond = (cond ==. const_ 0 a)

(!=.) :: Voodoo -> Voodoo -> Voodoo
a !=. b = (const_ 1 a) -. (a ==. b)
-- NOTE: check needed to make sure we
-- only have 0 or 1 in the multiplication.

type MemoTable = HMap.HashMap V.Vexp Voodoo

voodooFromVexpMemo :: MemoTable -> V.Vexp -> Either String (MemoTable, Voodoo)
voodooFromVexpMemo s vexp@(V.Vexp vx _ _ _ _ _) =
  case HMap.lookup vexp s of
    Nothing -> do (s',r) <- voodooFromVxNoMemo s vx
                  let s'' = HMap.insert vexp r s'
                  return (s'',r)
    Just r -> return (s, r)

voodooFromVxNoMemo :: MemoTable -> V.Vx -> Either String (MemoTable, Voodoo)

voodooFromVxNoMemo st (V.Load n) =
  do inname <- (case n of
                   Name (_:s:rest) -> Right $ Name $ s:rest
                   _ -> Left $ "need longer keypath to be consistent with ./Driver keypaths)")
     return $    (st, Project { outname=Name ["val"]
                          , inname
                          , vec = completeW $ Load n })

voodooFromVxNoMemo s (V.RangeV {V.rmin, V.rstep, V.rref}) =
  do (s',v) <- voodooFromVexpMemo s rref
     return $ (s', RangeV {rmin, rstep, rvec=completeW v})

voodooFromVxNoMemo s (V.RangeC {V.rmin, V.rstep, V.rcount}) =
  return $ (s, RangeC {rmin, rstep, rcount })

voodooFromVxNoMemo s (V.Binop { V.binop, V.left, V.right}) =
  do (s', l) <- voodooFromVexpMemo s left
     (s'', r) <- voodooFromVexpMemo s' right
     let t = case binop of
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
              V.Mod -> error "implement this?"
     return $ (s'',t)

voodooFromVxNoMemo s (V.Shuffle { V.shop,  V.shsource, V.shpos }) =
  do (s', source) <- voodooFromVexpMemo s shsource
     (s'', positions) <- voodooFromVexpMemo s' shpos
     return $ (s'', case shop of
                  V.Gather -> Binary { op=Gather, arg1=completeW source, arg2=completeW positions }
                  V.Scatter -> let scatterfold = completeW $ pos_ source
                               in Scatter { scattersource=completeW source, scatterfold, scatterpos=completeW positions })

voodooFromVxNoMemo s (V.Fold { V.foldop, V.fgroups, V.fdata}) =
  do (s', arg1) <- voodooFromVexpMemo s fgroups
     (s'', arg2) <- voodooFromVexpMemo s' fdata
     let op = case foldop of
               V.FSum -> FoldSum
               V.FMax -> FoldMax
               V.FMin -> FoldMin
               V.FSel -> FoldSelect
     return $ (s'', Binary { op, arg1=completeW arg1, arg2=completeW arg2 })

voodooFromVxNoMemo s (V.Partition {V.pdata, V.pivots}) =
  do (s', arg1) <- voodooFromVexpMemo s pdata
     (s'', arg2) <- voodooFromVexpMemo s' pivots
     return $ (s'', Binary {op=Partition, arg1=completeW arg1, arg2=completeW arg2 })

voodoosFromVexps :: [V.Vexp] -> Either String [Voodoo]
voodoosFromVexps vexps =
  do let solve (s, res) v = do (s', v') <- voodooFromVexpMemo s v
                               return (s', v':res)
     (_, res) <- foldM solve (HMap.empty,[]) vexps
     return $ res

vrefsFromVoodoos :: [Voodoo] -> Either String Log
vrefsFromVoodoos vecs =
  do let log0 = [(0, Load $ Name ["dummy"])]
     let state0 = ((HMap.empty, log0), undefined)
     ((_, finalLog),_) <- foldM process state0 vecs
     let post = tail $ reverse $ finalLog -- remove dummy, reverse
     --let tr = "\n--Vref output:\n" ++ groom post
     --return $ trace tr post
     return $ trace (show $ length post) post
       where process (state, _) v  = memVrefFromVoodoo state v

type LookupTable = HMap.HashMap Voodoo Int
type Log = [(Int, Vref)]
type State = (LookupTable, Log)

addToLog :: Log -> Vref -> (Log, Int)
addToLog log v = let (n, _) = head log
                 in ((n+1, v) : log, n+1)

--used to remember common expressions from before
memVrefFromVoodoo :: State -> Voodoo -> Either String (State, Int)
memVrefFromVoodoo state@(tab, log) vd =
  case HMap.lookup vd tab of
    Nothing -> do ((tab', log'), vref) <- vrefFromVoodoo state vd
                  let (log'', iden) = addToLog log' vref
                  let tab'' = HMap.insert vd iden tab'
                    in return $ ((tab'', log''), iden)
    Just iden -> Right ((tab, log), iden) -- nothing changes

vrefFromVoodoo :: State -> Voodoo -> Either String (State, Vref)

vrefFromVoodoo state (Load n) = return $ (state, Load n)

vrefFromVoodoo state (RangeC {rmin,rstep,rcount} ) =
  return $ (state, RangeC {rmin,rstep,rcount})

vrefFromVoodoo state (RangeV  {rmin,rstep,rvec=W (rvec,_)}) =
  do (state', n') <- memVrefFromVoodoo state rvec
     return $ (state', RangeV { rmin,rstep, rvec= n' })

vrefFromVoodoo state (Scatter {scattersource=W (scattersource,_)
                              , scatterfold=W (scatterfold,_)
                              , scatterpos=W (scatterpos,_) }) =
  do (state', n') <- memVrefFromVoodoo state scattersource
     (state'', n'') <- memVrefFromVoodoo state' scatterfold
     (state''', n''') <- memVrefFromVoodoo state'' scatterpos
     return $ (state''', Scatter {scattersource= n', scatterfold= n'', scatterpos= n'''})

vrefFromVoodoo state (Project {outname, inname, vec=W (vec,_)}) =
  do (state', n') <- memVrefFromVoodoo state vec
     return $ (state', Project { outname, inname, vec=n' })


vrefFromVoodoo state (Binary { op, arg1=W (arg1,_) , arg2=W (arg2,_) }) =
  do (state', n1) <- memVrefFromVoodoo state arg1
     (state'', n2) <- memVrefFromVoodoo state' arg2
     return $ (state'', Binary { op, arg1=n1, arg2=n2})

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

toList s_ = trace ("TODO implement toList for: " ++ show s_) undefined

printVd :: [(Int, [String])] -> String
printVd prs = join "\n" $ map makeline prs
  where makeline (iden, strs) =  join "," $ (show iden) : strs

dumpVref :: Log -> String
dumpVref prs = let (ids, vrefs) = unzip prs
                   lsts = map toList vrefs
               in printVd $ zip ids lsts

-- forces the printer to use our custom format rather than
-- a default
data Vdl = Vdl String
instance Show Vdl where
  show (Vdl s) = s

vdlFromVexps :: [V.Vexp] -> Config -> Either String Vdl
vdlFromVexps vexps _ =
  do voodoos <- voodoosFromVexps vexps
     vrefs <- vrefsFromVoodoos voodoos
     return $ Vdl $ dumpVref vrefs
