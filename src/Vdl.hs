module Vdl (vdlFromVexps) where

import Control.Monad(foldM)
import Name(Name(..))
import Data.Int
import Debug.Trace
--import Text.Groom
import GHC.Generics
import Data.String.Utils(join)
import Control.DeepSeq(NFData)
import qualified Vlite as V
import qualified Data.Map.Strict as Map
import Data.List (foldl')
import Config
import Prelude hiding (log)
import qualified Error as E
type Map = Map.Map

{-
Aims to cover the subset of VoodooStdLib we need for Monet plans.
Nothing in this type should be non-existant in Voodoo.
its function is to be easy to convert to text format.
-}
data Voodoo =
  Load Name
  | Project { outname::Name , inname::Name, vec :: Voodoo } -- full rename only. used right after load
  | Range { rmin::Int64, rstep::Int64, rvec :: Voodoo }
  | RangeC {rmin::Int64, rstep::Int64, rcount::Int64 }
  | Binary { op::Voodop, arg1::Voodoo, arg2::Voodoo  }
  | Scatter { scattersource::Voodoo, scatterfold::Voodoo, scatterpos::Voodoo }
  deriving (Eq,Show,Generic,Ord)
instance NFData Voodoo

const_ :: Int64 -> Voodoo -> Voodoo
const_ k v  = Range { rmin=k, rstep=0, rvec=v }

pos_ :: Voodoo -> Voodoo
pos_ v  = Range { rmin=0, rstep=1, rvec=v }

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
  deriving (Eq,Show,Generic,Ord)
instance NFData Voodop

(.^.) :: (Int,Int) -> (Int,Int) -> (Int,Int)
(a,b) .^. (a',b') = (max a a', b + b')

size :: Voodoo -> (Int, Int)
size (Load _) = (1,1)
size (Scatter ch1 ch2 ch3) = let (a,b) = (size ch1) .^. (size ch2) .^. (size ch3) in (a + 1, b + 1)
size (Range _ _ _) = (1,1)
size (RangeC _ _ _) = (1,1)
size (Project _ _ ch) = let (a,b) = (size ch) .^. (1,1) in (a+1, b+1)
size (Binary _ ch1 ch2) = let (a,b) = (size ch1) .^. (size ch2) in (a+1,b+1)

dagSize :: [Voodoo] -> (Int,Int)
dagSize outputs = let (a,b) = foldl' (.^.) (0,0) (map size outputs) in (a+1,b+(length outputs))

-- convenience expression library to translate more complex
-- all have type Voodoo -> Voodoo -> Voodoo
(>.) :: Voodoo -> Voodoo -> Voodoo
a >.  b = Binary { op=Greater, arg1=a, arg2=b }

(==.) :: Voodoo -> Voodoo -> Voodoo
a ==. b = Binary { op=Equals, arg1=a, arg2=b }

(<.) :: Voodoo -> Voodoo -> Voodoo
a <.  b = Binary { op=Greater, arg1=b, arg2=a } --notice argument swap

(||.) :: Voodoo -> Voodoo -> Voodoo
a ||. b = Binary { op=LogicalOr, arg1=a, arg2=b }

(>>.) :: Voodoo -> Voodoo -> Voodoo
a >>. b = Binary { op=BitShift, arg1=a, arg2=b }

(&&.) :: Voodoo -> Voodoo -> Voodoo
a &&. b = Binary { op=LogicalAnd, arg1=a, arg2=b }

(<=.) :: Voodoo -> Voodoo -> Voodoo
a <=. b = (a <. b) ||. (a ==. b)

(>=.) :: Voodoo -> Voodoo -> Voodoo
a >=. b = (a >. b) ||. (a ==. b)

(+.) :: Voodoo -> Voodoo -> Voodoo
a +. b = Binary { op=Add, arg1=a, arg2=b }

(-.) :: Voodoo -> Voodoo -> Voodoo
a -. b = Binary { op=Subtract, arg1=a, arg2=b }

(*.) :: Voodoo -> Voodoo -> Voodoo
a *. b = Binary { op=Multiply, arg1=a, arg2=b }

(/.) :: Voodoo -> Voodoo -> Voodoo
a /. b = Binary { op=Divide, arg1=a, arg2=b }

(|.) :: Voodoo -> Voodoo -> Voodoo
a |. b = Binary { op=BitwiseOr, arg1=a, arg2=b }

(?.) :: Voodoo -> (Voodoo,Voodoo) -> Voodoo
cond ?. (a,b) = ((const_ 1 a  -. negcond) *. a) +. (negcond *. b)
  where negcond = (cond ==. const_ 0 a)

(!=.) :: Voodoo -> Voodoo -> Voodoo
a !=. b = (const_ 1 a) -. (a ==. b)
-- NOTE: check needed to make sure we
-- only have 0 or 1 in the multiplication.

voodooFromVexp :: V.Vexp -> Either String Voodoo
voodooFromVexp (V.Load n) =
  do inname <- (case n of
                   Name (_:s:rest) -> Right $ Name $ s:rest
                   _ -> Left $ "need longer keypath to be consistent with ./Driver keypaths)")
     return $ Project { outname=Name ["val"]
                      , inname
                      , vec = Load n }
voodooFromVexp (V.RangeV {V.rmin, V.rstep, V.rref}) =
  do v <- voodooFromVexp rref
     return $ Range {rmin, rstep, rvec=v}

voodooFromVexp (V.RangeC {V.rmin, V.rstep, V.rcount}) =
  return $ RangeC {rmin, rstep, rcount }


voodooFromVexp (V.Binop { V.binop, V.left, V.right}) =
  do l <- voodooFromVexp left
     r <- voodooFromVexp right
     case binop of
       V.Gt -> Right $ l >. r
       V.Eq -> Right $ l ==. r
       V.Mul -> Right $ l *. r
       V.Sub -> Right $ l -. r
       V.Add -> Right $ l +. r
       V.Lt -> Right $ l <. r
       V.Leq -> Right $ l <=. r
       V.Geq -> Right $ l >=. r
       V.Min -> Right $ (l <=. r) ?. (l, r)
       V.Max -> Right $ (l >=. r) ?. (l, r)
       V.LogAnd -> Right $ (l &&. r)
       V.LogOr -> Right $ (l ||. r)
       V.Div -> Right $ (l /. r)
       V.BitShift -> Right $  (l >>. r)
       V.Neq -> Right $ (l !=. r)
       V.BitOr -> Right $ (l |. r)
       _ -> Left $ "binop not implemented: " ++ show binop

voodooFromVexp  (V.Shuffle { V.shop,  V.shsource, V.shpos }) =
  do source <- voodooFromVexp shsource
     positions <- voodooFromVexp shpos
     case shop of
       V.Gather -> Right $ Binary { op=Gather, arg1=source, arg2=positions }
       V.Scatter -> let scatterfold = pos_ source
                        in Right $ Scatter { scattersource=source, scatterfold, scatterpos=positions }
       _ -> Left $ "shop not implemented" ++ show shop

voodooFromVexp (V.Fold { V.foldop, V.fgroups, V.fdata}) =
  do arg1 <- voodooFromVexp fgroups
     arg2 <- voodooFromVexp fdata
     case foldop of
       V.FSum -> return $ Binary { op=FoldSum, arg1, arg2 }
       V.FMax -> return $ Binary { op=FoldMax, arg1, arg2 }
       V.FSel -> return $ Binary { op=FoldSelect, arg1, arg2 }
       s_ -> Left $ E.unexpected "fold operator" s_

voodooFromVexp (V.Partition {V.pdata, V.pivots}) =
  do arg1 <- voodooFromVexp pdata
     arg2 <- voodooFromVexp pivots
     return $ Binary {op=Partition, arg1, arg2 }

voodooFromVexp s_ = Left $ "implement me: " ++ show s_

{- the difference now is that
all pointers to other expressions now are explicit,
needed for serialization -}
data Vref  =
  VLoad Name
  | VProject { voutname::Name , vinname::Name, vvec :: Int } -- full rename only.
  | VRange  { vrmin :: Int64, vrstep :: Int64, vrvec :: Int }
  | VRangeC  { vrmin :: Int64, vrstep :: Int64, vrcount :: Int64 }
  | VBinary { vop :: Voodop, varg1 :: Int, varg2 :: Int }
  | VScatter { vscatterpos :: Int, vscatterfold::Int, vscattersource ::Int }
  deriving (Eq,Show,Generic)
instance NFData Vref

voodoosFromVexps :: [(V.Vexp, Maybe Name)] -> Either String [Voodoo]

voodoosFromVexps vexps = mapM (voodooFromVexp  . fst) vexps

vrefsFromVoodoos :: [Voodoo] -> Either String Log
vrefsFromVoodoos vecs =
  do let log0 = [(0, VLoad $ Name ["dummy"])]
     let state0 = ((Map.empty, log0), undefined)
     ((_, finalLog),_) <- foldM process state0 vecs
     let post = tail $ reverse $ finalLog -- remove dummy, reverse
     --let tr = "\n--Vref output:\n" ++ groom post
     --return $ trace tr post
     return $ trace (let s = dagSize  vecs in "vecs (depth,count): " ++ (show s) ++ "\npost len: " ++ (show $ length post)) post
       where process (state, _) v  = memVrefFromVoodoo state v

type LookupTable = Map Voodoo Int
type Log = [(Int, Vref)]
type State = (LookupTable, Log)

addToLog :: Log -> Vref -> (Log, Int)
addToLog log v = let (n, _) = head log
                 in ((n+1, v) : log, n+1)

--used to remember common expressions from before
memVrefFromVoodoo :: State -> Voodoo -> Either String (State, Int)
memVrefFromVoodoo state@(tab, log) vd =
  case Map.lookup vd tab of
    Nothing -> do ((tab', log'), vref) <- vrefFromVoodoo state vd
                  let (log'', iden) = addToLog log' vref
                  let tab'' = Map.insert vd iden tab'
                    in return $ ((tab'', log''), iden)
    Just iden -> Right ((tab, log), iden) -- nothing changes

vrefFromVoodoo :: State -> Voodoo -> Either String (State, Vref)

vrefFromVoodoo state (Load n) = return $ (state, VLoad n)

vrefFromVoodoo state (RangeC  { rmin , rstep, rcount })=
  return $ (state, VRangeC {vrmin=rmin, vrstep=rstep, vrcount=rcount})

vrefFromVoodoo state (Range  { rmin , rstep, rvec  }) =
  do (state', n') <- memVrefFromVoodoo state rvec
     return $ (state', VRange { vrmin=rmin, vrstep=rstep, vrvec=n' })

vrefFromVoodoo state (Scatter {scattersource, scatterfold, scatterpos }) =
  do (state', n') <- memVrefFromVoodoo state scattersource
     (state'', n'') <- memVrefFromVoodoo state' scatterfold
     (state''', n''') <- memVrefFromVoodoo state'' scatterpos
     return $ (state''', VScatter {vscattersource=n', vscatterfold=n'', vscatterpos=n'''})

vrefFromVoodoo state (Project {outname, inname, vec}) =
  do (state', n') <- memVrefFromVoodoo state vec
     return $ (state', VProject { voutname=outname, vinname=inname, vvec=n' })


vrefFromVoodoo state (Binary { op , arg1 , arg2 }) =
  do (state', n1) <- memVrefFromVoodoo state arg1
     (state'', n2) <- memVrefFromVoodoo state' arg2
     return $ (state'', VBinary {vop=op, varg1=n1, varg2=n2})


{- now a list of strings -}
toList :: Vref -> [String]

-- for printing: remove sys (really, we want the prefix only_
toList (VLoad (Name lst)) = ["Load", show cleanname]
  where cleanname = Name (if head lst == "sys" then tail lst else lst)

toList (VProject {voutname, vinname, vvec}) =
  ["Project",show voutname, "Id " ++ show vvec, show vinname ]

toList (VRange { vrmin, vrstep, vrvec }) =
  {- for printing: hardcoded length 2 billion right now,
  since backend only materializes what's needed -}
  ["RangeV", "val", show vrmin, "Id " ++ show vrvec, show vrstep]

toList (VRangeC { vrmin, vrstep, vrcount }) =
  ["RangeC", "val", show vrmin, show vrcount, show vrstep]

toList (VBinary { vop, varg1, varg2}) =
  case vop of
    Gather -> [svop, id1, id2, "val"]
    _ ->      [svop, "val", id1, "val", id2, "val" ]
  where svop = show vop
        id1 = "Id " ++ show varg1
        id2 = "Id " ++ show varg2

toList (VScatter { vscattersource, vscatterfold, vscatterpos } ) =
  ["Scatter", id1, id2, "val", id3, "val"]
  where id1 = "Id " ++ show vscattersource
        id2 = "Id " ++ show vscatterfold
        id3 = "Id " ++ show vscatterpos

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

vdlFromVexps :: [(V.Vexp, Maybe Name)] -> Config -> Either String Vdl
vdlFromVexps vexps _ =
  do voodoos <- voodoosFromVexps vexps
     vrefs <- vrefsFromVoodoos voodoos
     return $ Vdl $ dumpVref vrefs
