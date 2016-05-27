module Voodoo (Voodoo(..)
               , fromString
               ,Vref(..)
               ) where

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
Meant mostly so that it is easy to convert  to text format.
-}
data Voodoo =
  Load Name
  | Range { rmin::Int64, rstep::Int64 }
  | Binary { op::Voodop, arg1::Voodoo, arg2::Voodoo  }
  deriving (Eq,Show,Generic)
instance NFData Voodoo

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
  | Gather
  | FoldSelect
  | FoldMax
  | FoldSum
  | FoldMin
  | FoldCount
  deriving (Eq,Show,Generic,Ord)
instance NFData Voodop


fromVexp :: V.Vexp -> Either String Voodoo

fromVexp (V.Load n) = return $ Load n
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
  do arg1 <- fromVexp shsource
     arg2 <- fromVexp shpos
     case shop of
       V.Gather -> return $ Binary { op=Gather, arg1, arg2 }
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
  | VRange  { vrmin :: Int64, vrstep :: Int64 }
  | VBinary { vop :: Voodop, varg1 :: Int, varg2 :: Int }
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
     let processed = foldl process log0 vecs
     let post = tail $ reverse $ processed -- remove dummy, reverse
     let tr = "\n--Vref output:\n" ++ groom post
     return $ trace tr post
       where process log vec  = let (newl, _) = vrefFromVoodoo log vec
                                in newl



addToLog :: [(Int, Vref)] -> Vref -> ([(Int, Vref)], Int)
addToLog log v = let (n, _) = head log
                 in ((n+1, v) : log, n+1)


vrefFromVoodoo :: [(Int, Vref)] -> Voodoo -> ([(Int, Vref)], Int)

vrefFromVoodoo log v@(Load n) = addToLog log (VLoad n)
vrefFromVoodoo log v@(Range  { rmin , rstep  }) =
  addToLog log (VRange { vrmin=rmin, vrstep=rstep })

vrefFromVoodoo log v@(Binary { op , arg1 , arg2 }) =
  let (newlog, newn) = vrefFromVoodoo log arg1
      (newlog', newn') = vrefFromVoodoo newlog arg2
      newbinop = VBinary {vop=op, varg1=newn, varg2=newn'}
      in addToLog newlog' newbinop

{- now a list of strings -}
toVcsv :: Vref -> Either String [String]
toVcsv (VLoad n) = Right ["Load", show n]
toVcsv (VRange { vrmin, vrstep }) = Right ["Range", show vrmin, show vrstep]
toVcsv (VBinary { vop, varg1, varg2}) =
  do opstr <- printVoodop vop
     return $ [ opstr
              , "Id " ++ show varg1
              , "Id " ++ show varg2 ]

{- string needs to be parsable by interpreter.h -}
printVoodop :: Voodop -> Either String String
printVoodop op =
  case Map.lookup op strings of
    Nothing -> Left $ "cannot find string for " ++ show op
    Just x -> Right  x
  where strings = Map.fromList $
          [ (Greater , "Greater")
          ,(LogicalAnd , "LogicalAnd")
          ,(LogicalOr , "LogicalOr")
          ,(BitwiseAnd , "BitwiseAnd")
          ,(BitwiseOr , "BitwiseOr")
          ,(Equals , "Equals")
          ,(Add , "Add")
          ,(Subtract , "Subtract")
          ,(Greater , "Greater")
          ,(Multiply , "Multiply")
          ,(Divide , "Divide")
          ,(Gather , "Gather")
          ,(FoldMax , "foldMax")
          ,(FoldSum , "foldSum")
          ,(FoldMin , "foldMin")
          ,(FoldCount , "foldCount")
          ]
