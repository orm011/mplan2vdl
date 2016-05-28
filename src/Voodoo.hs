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
  | Gather { input::Voodoo, positions::Voodoo }
  deriving (Eq,Show,Generic)
instance NFData Voodoo

data Voodop =
  LogicalAnd
  | LogicalOr
  | LogicalNot
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
  deriving (Eq,Show,Generic,Ord)
instance NFData Voodop


fromVexp :: V.Vexp -> Either String Voodoo

fromVexp (V.Load n) = return $ Project { outname=Name ["val"], vec = Load n }
fromVexp (V.Range {V.rmin, V.rstep}) = return $ Range {rmin, rstep}
fromVexp (V.Binop { V.bop, V.bleft, V.bright}) =
  do left <- fromVexp bleft
     right <- fromVexp bright
     case bop of
       V.Gt -> Right $ Binary { op=Greater, arg1=left, arg2=right }
       V.Lt -> Right $ Binary { op=Greater, arg1=right,arg2=left } --notice swap
       V.Eq -> Right $ Binary { op=Equals, arg1=left, arg2=right }
       V.Mul -> Right $ Binary { op=Multiply, arg1=left, arg2=right }
       _ -> Left $ "bop not implemented" ++ show bop

fromVexp  (V.Shuffle { V.shop,  V.shsource, V.shpos }) =
  do input <- fromVexp shsource
     positions <- fromVexp shpos
     case shop of
       V.Gather -> Right $ Gather { input, positions }
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
  | VGather { vinput :: Int, vpositions :: Int }
  deriving (Eq,Show,Generic)
instance NFData Vref

voodooFromString :: String -> Either String [Voodoo]
voodooFromString mplanstring =
  do vexps <- V.fromString mplanstring
     let vecs = mapM (fromVexp  . fst) vexps
     let tr = case vecs of
                Left err -> "\n--Error at Voodoo stage:\n" ++ err
                Right g -> "\n--Voodoo output:\n" ++ groom g
     trace tr vecs


fromString :: String -> Either String [(Int, Vref)]
fromString = vrefFromString

vrefFromString :: String -> Either String [(Int, Vref)]
vrefFromString str =
  do vecs <- voodooFromString str
     let log0 = [(0, VLoad $ Name ["dummy"])]
     processed <- foldM process log0 vecs
     let post = tail $ reverse $ processed -- remove dummy, reverse
     let tr = "\n--Vref output:\n" ++ groom post
     return $ trace tr post
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

vrefFromVoodoo log v@(Gather { input, positions }) =
  do (log', newn') <- vrefFromVoodoo log input
     (log'', newn'') <- vrefFromVoodoo log' positions
     let newg = VGather { vinput=newn', vpositions=newn'' }
     return $ addToLog log'' newg

vrefFromVoodoo log s_ = Left $ "you need to implement: " ++ groom s_


{- now a list of strings -}
toList :: Vref -> Either String [String]

-- for printing: remove sys (really, we want the prefix only_
toList (VLoad (Name lst)) = Right ["Load", show cleanname]
  where cleanname = Name (if head lst == "sys" then tail lst else lst)

toList (VProject voutname vvec) = Right ["Project",show voutname, "Id " ++ show vvec]

toList (VRange { vrmin, vrstep }) =
  {- for printing: hardcoded length 4billion right now, since backend only materializes what's needed -}
  Right ["Range", "val", show vrmin, show 4000000000, show vrstep]

toList (VBinary { vop, varg1, varg2}) =
  return $ [ show vop
              , "val"
              , "Id " ++ show varg1
              , "val"
              , "Id " ++ show varg2
              , "val" ]

toList (VGather { vinput, vpositions }) =
  return $ [ "Gather"
           , "Id " ++ show vinput
           , "Id " ++ show vpositions
           , "val"]

toList s_ = Left $ "TODO implement toList for: " ++ show s_

printVd :: [(Int, [String])] -> String
printVd prs = join "\n" $ map makeline prs
  where makeline (id, strs) =  join "," $ (show id) : strs

dumpVref :: [(Int, Vref)] -> Either String String
dumpVref prs = let (ids, vrefs) = unzip prs
               in do lsts <- mapM toList vrefs
                     return $ printVd $ zip ids lsts

vdlFromMplan :: String -> Either String String
vdlFromMplan mplanstring =
  do vrefs <- vrefFromString mplanstring
     let vdl = dumpVref vrefs
     let tr = case vdl of
                 Left err -> "\n--Error at Vdl stage:\n" ++ err
                 Right g -> "\n--Vdl output:\n" ++ groom g
     trace tr vdl
