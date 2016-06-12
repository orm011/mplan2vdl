module Dot (toDotString) where

import TreeParser
import Name ()
import Data.List(foldl')
import Text.Printf (printf)
import Data.String.Utils(join,replace)
import Data.List.NonEmpty 
import Prelude hiding (map,last,reverse)
import qualified Prelude as P

--import Debug.Trace

data Dot = Dot (NonEmpty (Int, String, String)) [(Int,Int)]

cat :: NonEmpty a -> NonEmpty a -> NonEmpty a
cat a b = foldr (<|) b a -- b is the accumulate

toDot :: Int -> TRel -> (Int, Dot)

toDot n (TNode { relop, children, arg_lists }) =
  let firststr = case arg_lists of
        [] -> ""
        s : _ -> s
      merge (m, directChildrenIds, labels1, edges1) child =
        let (m', (Dot labels2 edges2)) = toDot m child
            (l,_,_) = last labels2 -- the direct child is the last element
            ans = (m', l:directChildrenIds, cat labels2 labels1 , edges2 ++ edges1)
        in ans
      initial = (n+1, [], (n,relop,firststr):|[], [])
      (n', childids, acclabels, accedges) = foldl' merge initial children
      chedges = P.map (\ch -> (n,ch)) (P.reverse childids) -- the fold has reversed them
  in (n', Dot acclabels (chedges ++ accedges))


toDot n (TLeaf name  attr) = (n+1, Dot ((n,"table " ++ show name,attr) :| []) [])

fromTRelToDot :: TRel -> Dot
fromTRelToDot r = snd $ toDot 0 r

fromDotToDotString :: String -> Dot -> String
fromDotToDotString _ (Dot labels edges) =
  let prologue = [printf "digraph foo {"]
      epilogue = ["}"]
      printnodelabel (n, labelname, _) =
        printf "%d [ label = \"[%d] %s\" ]" n n labelname
      printnodemeta (n, _, attrstr) =
        let attrnode = (show n) ++ "00000" -- hack to avoid node node collision
        in ( printf "%s [ label=\"%s\" shape = \"box\" color=\"blue\" ]" attrnode (replace "\"" "" attrstr)
           , printf "%d -> %s [style=\"dotted\" color=\"blue\"]" n attrnode
           )
      printedge (a,b) = printf "%d -> %d" a b
      operlabels = P.map printnodelabel (P.reverse $ toList labels)
      (metalabels,metaedges) = P.unzip (P.map printnodemeta $ toList labels)
      operedges = (P.map printedge edges)
      alllines = foldl' (++) [] [prologue, metalabels, operlabels, metaedges, operedges, epilogue]
  in  join "\n" alllines

toDotString :: String -> TRel -> String
toDotString name rel =
  let dot = fromTRelToDot rel
  in fromDotToDotString name dot

-- issues using nonempty: concat. map,fold collision. <| vs :|.
-- but: head, last etc. all don't need to match.
