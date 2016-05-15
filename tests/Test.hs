import Test.Tasty
{- import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC -}
import Test.Tasty.HUnit
import Parser(parse, fromString)
import Scanner(scan)
import Data.Either(isRight,partitionEithers)
import qualified Data.Text as T
import Configuration(defaultConfiguration)
import Text.Groom
import Debug.Trace
import Data.List(intercalate)

main :: IO ()
main = do base <- readFile "tests/monet_test_cases.txt"
          tpch <- readFile "tests/tpch_query_plans.txt"
          defaultMain $ testGroup "Tests"
           [ testGroup "AdHocTests" (get_test_cases base)
           , testGroup "TPCHTests" (get_test_cases tpch)
            ]

toTestCase :: (String, String) -> TestTree
toTestCase (a, b)  =
  let zpd = zip [1..] (T.splitOn (T.pack "\n") (T.pack b))
      numbered_plan = map (\(n, line) -> (show n) ++ " " ++ (T.unpack line)) zpd
  in
    testCase ("------\n" ++  a ++ "\n\n" ++ (intercalate "\n" numbered_plan) ++"\n------\n") (let prs = fromString b in (isRight  prs)  @? groom prs)

get_test_cases :: String  -> [TestTree]
get_test_cases s = {- the first element after split is a "", because we start with a plan, so must do tail -}
  let raw_pairs =
        case (T.splitOn (T.pack "--TEST--") (T.pack s)) of
          [] -> []
          _ : rest -> rest
      to_pairs [x,y] = (T.unpack . T.strip $ x, T.unpack . T.strip $ y)
  in map (toTestCase . to_pairs . (T.splitOn (T.pack ";"))) raw_pairs


-- unitTests = testGroup "Unit tests"
--   [ -- testCase "List comparison (different length)" $
--   --     [1, 2, 3] `compare` [1,2] @?= GT

--   -- -- the following test does not hold
--   -- , testCase "List comparison (same length)" $
--   --     [1, 2, 3] `compare` [1,2,2] @?= LT
--   ]
