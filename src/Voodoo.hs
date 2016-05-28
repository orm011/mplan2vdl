module Voodoo (Voodoo(..)
               ,fromString
               ,Vref(..)
               ,vdlFromMplan
               ) where

import Control.Monad(foldM, mapM, void)
import Name(Name(..))
import Data.Int
import Debug.Trace
import Text.Groom
import GHC.Generics
import Data.String.Utils(join)
import Control.DeepSeq(NFData,($!!))
import qualified Vlite as V
import qualified Data.Map.Strict as Map
import Data.List (foldl')

type Map = Map.Map

{-
Aims to cover the subset of VoodooStdLib we need for Monet plans.
Nothing in this type should be non-existant in Voodoo.
its function is to be easy to convert to text format.
-}
data Voodoo =
  Load Name
  | Project { outname::Name , vec :: Voodoo } -- full rename only. used right after load
  | Range { rmin::Int64, rstep::Int64 }
  | Binary { op::Voodop, arg1::Voodoo, arg2::Voodoo  }
  deriving (Eq,Show,Generic)
instance NFData Voodoo

const_ k  = Range { rmin=k, rstep=0 }

data Voodop =
  LogicalAnd
  | LogicalOr
  | BitwiseAnd
  | BitwiseOr
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
  deriving (Eq,Show,Generic)
instance NFData Voodop

(.^.) :: (Int,Int) -> (Int,Int) -> (Int,Int)
(a,b) .^. (a',b') = (max a a', b + b')

size :: Voodoo -> (Int, Int)
size (Load _) = (1,1)
size (Range _ _) = (1,1)
size (Project _ ch) = let (a,b) = (size ch) .^. (1,1) in (a+1, b+1)
size (Binary _ ch1 ch2) = let (a,b) = (size ch1) .^. (size ch2) in (a+1,b+1)

dagSize :: [Voodoo] -> (Int,Int)
dagSize outputs = let (a,b) = foldl' (.^.) (0,0) (map size outputs) in (a+1,b+(length outputs))

-- convenience expression library to translate more complex expressions
a >.  b = Binary { op=Greater, arg1=a, arg2=b }
a ==. b = Binary { op=Equals, arg1=a, arg2=b }
a <.  b = Binary { op=Greater, arg1=b, arg2=a } --notice argument swap
a ||. b = Binary { op=LogicalOr, arg1=a, arg2=b }
a &&. b = Binary { op=LogicalAnd, arg1=a, arg2=b }
a <=. b = (a <. b) ||. (a ==. b)
a >=. b = (a >. b) ||. (a ==. b)
a +. b = Binary { op=Add, arg1=a, arg2=b }
a -. b = Binary { op=Subtract, arg1=a, arg2=b }
a *. b = Binary { op=Multiply, arg1=a, arg2=b }
cond ?. (a,b) = (const_ 1 -. negcond) *. a +. negcond *. b
  where negcond = (cond ==. const_ 0) -- NOTE: needed to make sure we only have 0 or 1 in the multiplication.

fromVexp :: V.Vexp -> Either String Voodoo

fromVexp (V.Load n) = return $ Project { outname=Name ["val"], vec = Load n }
fromVexp (V.Range {V.rmin, V.rstep}) = return $ Range {rmin, rstep}
fromVexp (V.Binop { V.bop, V.bleft, V.bright}) =
  do left <- fromVexp bleft
     right <- fromVexp bright
     case bop of
       V.Gt -> Right $ left >. right
       V.Eq -> Right $ left ==. right
       V.Mul -> Right $ left *. right
       V.Sub -> Right $ left -. right
       V.Add -> Right $ left +. right
       V.Lt -> Right $ left <. right
       V.Leq -> Right $ left <=. right
       V.Geq -> Right $ left >=. right
       V.Min -> Right $ (left <=. right) ?. (left, right)
       V.Max -> Right $ (left >=. right) ?. (left, right)
       V.LogAnd -> Right $ (left &&. right)
       V.LogOr -> Right $ (left ||. right)
       _ -> Left $ "bop not implemented: " ++ show bop

fromVexp  (V.Shuffle { V.shop,  V.shsource, V.shpos }) =
  do input <- fromVexp shsource
     positions <- fromVexp shpos
     case shop of
       V.Gather -> Right $ Binary { op=Gather, arg1=input, arg2=positions }
       _ -> Left $ "shop not implemented" ++ show shop

fromVexp (V.Fold { V.foldop, V.fdata, V.fgroups }) =
  do arg1 <- fromVexp fdata
     arg2 <- fromVexp fgroups
     case foldop of
       V.FSum -> return $ Binary { op=FoldSum, arg1, arg2 }
       V.FMax -> return $ Binary { op=FoldMax, arg1, arg2 }
       V.FSel -> return $ Binary { op=FoldSelect, arg1, arg2 }

fromVexp s_ = Left $ "implement me: " ++ show s_

{- the difference now is that
all pointers to other expressions now are explicit,
needed for serialization -}
data Vref  =
  VLoad Name
  | VProject { voutname::Name , vvec :: Int } -- full rename only.
  | VRange  { vrmin :: Int64, vrstep :: Int64 }
  | VBinary { vop :: Voodop, varg1 :: Int, varg2 :: Int }
  deriving (Eq,Show,Generic)
instance NFData Vref

voodooFromString :: String -> Either String [Voodoo]
voodooFromString mplanstring =
  do vexps <- V.fromString mplanstring
     let vecs = mapM (fromVexp  . fst) $!! vexps
     -- let tr = case vecs of
     --            Left err -> "\n--Error at Voodoo stage:\n" ++ err
     --            Right g -> "\n--Voodoo output:\n" ++ groom g
     vecs


fromString :: String -> Either String [(Int, Vref)]
fromString = vrefFromString

vrefFromString :: String -> Either String [(Int, Vref)]
vrefFromString str =
  do vecs <- voodooFromString str
     let log0 = [(0, VLoad $ Name ["dummy"])]
     processed <- foldM process log0 vecs
     let post = tail $ reverse $ processed -- remove dummy, reverse
     --let tr = "\n--Vref output:\n" ++ groom post
     --return $ trace tr post
     return $ trace (let s = dagSize  vecs in "vecs (depth,count): " ++ (show s) ++ "\npost len: " ++ (show $ length post)) post
       where process log vec  = do (newl, _) <- vrefFromVoodoo log vec
                                   return newl



addToLog :: [(Int, Vref)] -> Vref -> ([(Int, Vref)], Int)
addToLog log v = let (n, _) = head log
                 in ((n+1, v) : log, n+1)


vrefFromVoodoo :: [(Int, Vref)] -> Voodoo -> Either String ([(Int, Vref)], Int)

vrefFromVoodoo log v@(Load n) = return $ addToLog log (VLoad n)
vrefFromVoodoo log v@(Project outname vec) =
  do (log', n') <- vrefFromVoodoo log vec
     return $ addToLog log' (VProject { voutname=outname, vvec=n' } )

vrefFromVoodoo log v@(Range  { rmin , rstep  }) =
  return $ addToLog log (VRange { vrmin=rmin, vrstep=rstep })

vrefFromVoodoo log v@(Binary { op , arg1 , arg2 }) =
  do (newlog, newn) <- vrefFromVoodoo log arg1
     (newlog', newn') <- vrefFromVoodoo newlog arg2
     let newbinop = VBinary {vop=op, varg1=newn, varg2=newn'}
     return $ addToLog newlog' newbinop

vrefFromVoodoo log s_ = Left $ "you need to implement: " ++ groom s_


{- now a list of strings -}
toList :: Vref -> [String]

-- for printing: remove sys (really, we want the prefix only_
toList (VLoad (Name lst)) = ["Load", show cleanname]
  where cleanname = Name (if head lst == "sys" then tail lst else lst)

toList (VProject voutname vvec) = ["Project",show voutname, "Id " ++ show vvec]

toList (VRange { vrmin, vrstep }) =
  {- for printing: hardcoded length 4billion right now, since backend only materializes what's needed -}
  ["Range", "val", show vrmin, show 4000000000, show vrstep]

toList (VBinary { vop, varg1, varg2}) =
  case vop of
    Gather -> [svop, id1, id2, "val"]
    _ ->      [svop, "val", id1, "val", id2, "val" ]
  where svop = show vop
        id1 = "Id " ++ show varg2
        id2 = "Id " ++ show varg1

toList s_ = trace ("TODO implement toList for: " ++ show s_) undefined

printVd :: [(Int, [String])] -> String
printVd prs = join "\n" $ map makeline prs
  where makeline (id, strs) =  join "," $ (show id) : strs

dumpVref :: [(Int, Vref)] -> String
dumpVref prs = let (ids, vrefs) = unzip prs
                   lsts = map toList vrefs
               in printVd $ zip ids lsts

vdlFromMplan :: String -> Either String String
vdlFromMplan mplanstring =
  do vrefs <- vrefFromString mplanstring
     return $ let vdl = dumpVref vrefs
              in vdl
