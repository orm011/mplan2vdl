{-# LANGUAGE ScopedTypeVariables #-}

module Dot (toDotString) where
import TreeParser
import Name ()
import Data.List(foldl')
import Text.Printf (printf)
import Data.String.Utils(replace)
import Data.List.NonEmpty hiding (map, last, reverse,zip)
import qualified Data.List.NonEmpty as N
import Prelude hiding (map,last,reverse)
import qualified Prelude as P
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C


--import Debug.Trace

data Dot = Dot (NonEmpty (Int, B.ByteString, [B.ByteString])) [(Int,Int)]

cat :: NonEmpty a -> NonEmpty a -> NonEmpty a
cat a b = foldr (<|) b a -- b is the accumulate

toDot :: Int -> TRel -> (Int, Dot)

toDot n (TNode { relop, children, arg_lists }) =
  let merge (m, directChildrenIds, labels1, edges1) child =
        let (m', (Dot labels2 edges2)) = toDot m child
            (l,_,_) = N.last labels2 -- the direct child is the last element
            ans = (m', l:directChildrenIds, cat labels2 labels1 , edges2 ++ edges1)
        in ans
      initial = (n+1, [], (n,relop,arg_lists):|[], [])
      (n', childids, acclabels, accedges) = foldl' merge initial children
      chedges = P.map (\ch -> (n,ch)) (P.reverse childids) -- the fold has reversed them
  in (n', Dot acclabels (chedges ++ accedges))


toDot n (TLeaf name  attr) = (n+1, Dot ((n, B.append "table " (C.pack $ show name), [attr]) :| []) [])

fromTRelToDot :: TRel -> Dot
fromTRelToDot r = snd $ toDot 0 r

fromDotToDotString :: B.ByteString -> Dot -> B.ByteString
fromDotToDotString gname (Dot labels edges) =
  let (prologue :: [B.ByteString]) = P.map C.pack ([printf ("digraph \"%s\" {" ::String) (C.unpack gname) ]::[String])
      (epilogue :: [B.ByteString])  = P.map C.pack (["}"] ::[String])
      printnodelabel (n, labelname, _) =
        C.pack (printf ("%d [ label = \"[%d] %s\" ]"::String) n n $ C.unpack labelname)
      printnodemeta (n, _, attrs) =
        do (attr,i) <- zip attrs [0..]
           let cleanattr = replace " ," ", "  $ replace "\"" "" (C.unpack attr) --quotations confuse me and confuse dot
           let attrnode = show $ (n + 1) * 10000 + i -- +1 bc zero id
           return $ ( C.pack $ printf ("%s [ label=\"%s\" shape = \"box\" color=\"blue\" ]"::String) attrnode cleanattr
                    , C.pack $ printf ("%d -> %s [style=\"dotted\" color=\"blue\"]"::String) n attrnode
                    )
      printedge (a,b) = C.pack $ printf ("%d -> %d"::String)  a  b
      operlabels = P.map printnodelabel (P.reverse $ toList labels)
      (metalabels,metaedges) = P.unzip (P.foldMap printnodemeta $ toList labels)
      operedges = (P.map printedge edges)
      (alllines :: [B.ByteString]) = foldl' (++) [] [prologue, metalabels, operlabels, metaedges, operedges, epilogue]
  in  B.intercalate "\n" alllines

toDotString :: B.ByteString -> TRel -> B.ByteString
toDotString name rel =
  let dot = fromTRelToDot rel
  in fromDotToDotString name dot

-- issues using nonempty: concat. map,fold collision. <| vs :|.
-- but: head, last etc. all don't need to match.
