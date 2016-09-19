module Name (Name(..)
            ,NameTable {-only export the ADT -}
            ,TypeSpec(..)
            ,empty
            ,insert
            ,insertWeak
            ,lookup
            ,lookup_err
            ,fromList
            ,toList
            ,get_last
            ) where

--import Data.String.Utils(join)
import qualified Data.Map.Strict as Map
import GHC.Generics (Generic)
import Text.Printf
import Prelude hiding (lookup)
import Control.DeepSeq(NFData)
import Data.Data
import Data.Hashable
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C
import Data.List (foldl')
--import qualified Data.ByteString.Builder as Bld

{- eg decimal(15,2) , or smallint  -}
data TypeSpec = TypeSpec { tname :: B.ByteString
                         , tparams :: [Integer] } deriving (Eq,Show,Generic)

instance NFData TypeSpec


{- the current implementation
works correctly under three key assumptions about the input:

1) we insert fully qualified names only,
2) we may query on suffixes of the inserted names
3) the client will query on suffixes only when they are unambiguous.

Right now, upon inserting truly ambigous names
(ie those where one is a prefix of another), we will return an error msg
This won't happen if 1 is true.

We assume all lookups are either successful, or there was an error.
(because we know our application should always insert the value before querying)

-}
data Name = Name [B.ByteString] deriving (Eq, Generic, Ord, Data)
instance Hashable Name
instance Show Name where
  show (Name lst) = C.unpack (C.intercalate "." lst)

-- gets the last piece of the name as a single name
get_last :: Name -> Name
get_last (Name segs) = Name [last segs]

type Map = Map.Map
instance NFData Name

data NameTable v = NameTable (Map [B.ByteString] v)

instance Show (NameTable t) where
  show (NameTable m) = let (revnames,_) = unzip (Map.toList m)
                       in show $ map (Name . reverse) revnames


fromList :: [(Name,a)] -> NameTable a
fromList prs = foldl' (\tab (n,a) -> insertWeak n a tab) empty prs

toList :: NameTable a -> [(Name,a)]
toList (NameTable tab) =
  let prs = Map.toList tab
  in map (\(bs, vexp) -> (Name (reverse bs), vexp)) prs

empty :: NameTable v
empty = NameTable Map.empty

isprefix :: (Eq a) => [a] -> [a] -> Bool
isprefix [] _  = True
isprefix _ [] = False
isprefix (a:resta) (b:restb) =
  if a == b then isprefix resta restb else False

lookup_err :: Name -> NameTable v -> (Name, v)
lookup_err n tab =
  case lookup n tab of
    Left m -> error m
    Right ans -> ans

lookup :: Name -> NameTable v -> Either String (Name, v)
lookup n@(Name lst) tab@(NameTable nt) =
  let reversed = reverse lst
      notfound = Left $  "no name: " ++ show n ++ "  in scope: " ++ show tab
  in case Map.lookupGE reversed nt of
    Just (candidate, val) ->
      if isprefix reversed candidate {- found one match -}
      then
        case Map.lookupGT candidate nt of {- check exactly one-}
          Just (next, _) ->
            if isprefix reversed next
            then
              let can1 = show $ (Name candidate)
                  can2 = show $ (Name next)
              in Left $ printf "Ambiguous name resolution for %s in %s: %s and %s both match." (show n) (show tab) can1 can2
            else Right (Name $ reverse candidate, val) {- exactly one -}
          Nothing ->  Right (Name $ reverse candidate, val) {- exactly one -}
      else notfound
    Nothing -> notfound

insert :: Name -> v -> NameTable v -> NameTable v
insert n@(Name lst) val (NameTable nt) =
  let sc = show $ map (Name . fst) $ Map.toAscList nt
      reversed = reverse lst
  in case Map.lookup reversed nt of
    Just  _ -> error $ printf "Scope %s already has %s" sc $ show n
    Nothing -> NameTable $ Map.insert reversed val nt

-- does not check for collisions.
insertWeak :: Name -> v -> NameTable v -> NameTable v
insertWeak (Name lst) val (NameTable nt) =
  let reversed = reverse lst
      in NameTable $ Map.insert reversed val nt
