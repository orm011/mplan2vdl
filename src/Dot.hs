module Dot (toDotString) where

import TreeParser
import Name ()
import Data.List(foldl')
import Text.Printf (printf)
import Data.String.Utils(join)
import Data.List.NonEmpty
import Prelude hiding (map,last)
import qualified Prelude as P
--import Debug.Trace

data Dot = Dot (NonEmpty (Int, String)) [(Int,Int)]

cat :: NonEmpty a -> NonEmpty a -> NonEmpty a
cat a b = foldr (<|) b a -- b is the accumulate

toDot :: Int -> TRel -> (Int, Dot)
toDot n (TNode { relop, children }) =
  let merge (m, directChildrenIds, labels1, edges1) child =
        let (m', (Dot labels2 edges2)) = toDot m child
            (l,_) = last labels2 -- the direct child is the last element
            ans = (m', l:directChildrenIds, cat labels2 labels1 , edges2 ++ edges1)
        in ans
      initial = (n+1, [], (n,relop):|[], [])
      (n', childids, acclabels, accedges) = foldl' merge initial children
      chedges = P.map (\ch -> (n,ch)) childids
  in (n', Dot acclabels (chedges ++ accedges))


toDot n (TLeaf name  _) = (n+1, Dot ((n,show name) :| []) [])

fromTRelToDot :: TRel -> Dot
fromTRelToDot r = snd $ toDot 0 r

fromDotToDotString :: String -> Dot -> String
fromDotToDotString _ (Dot labels edges) =
  let before = printf "digraph foo {"
      after = "}"
      printlabel (n,labelname) = printf "%d [ label = \"%s\" ]" n labelname
      printedge (a,b) = printf "%d -> %d" a b
      alllines = [before] ++ (P.map printlabel (toList labels)) ++ (P.map printedge edges) ++ [after]
  in  join "\n" alllines

toDotString :: String -> TRel -> String
toDotString name rel =
  let dot = fromTRelToDot rel
  in fromDotToDotString name dot

-- issues using nonempty: concat. map,fold collision. <| vs :|.
-- but: head, last etc. all don't need to match.
