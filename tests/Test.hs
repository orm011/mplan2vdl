import Test.Tasty
{- import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC -}
import Test.Tasty.HUnit
import Parser(parse, fromString)
import Scanner(scan)
import Data.Either(isRight,partitionEithers, rights)
import qualified Data.Text as T
import Configuration(defaultConfiguration)
import Text.Groom
import Debug.Trace
import Data.List(intercalate)

main :: IO ()
main = do base <- readFile "tests/ad_hoc_tests.txt"
          tpch <- readFile "tests/tpch_query_plans.txt"
          detailed  <- readFile "tests/detailed_tests.txt"
          defaultMain $ testGroup "Tests"
           [ testGroup "AdHocParseTests" (makeParseTestTree base) {-check only parses ok-}
           , testGroup "TPCHParseTests" (makeParseTestTree tpch) {- checks only parses ok -}
           , testGroup "DetailedParseTests" (makeParseTestTree detailed)
--           , testGroup "MPlanTests" (makeMplanTestTree detailed)  {- tests successful conversion to mplan  -}
            ]

makeTestName :: String ->  String -> String
makeTestName name plantext =
  let zpd = zip [1..] (T.splitOn (T.pack "\n") (T.pack plantext))
      numline (num, packedline) = (show num) ++ (if num < 10 then " " else "") ++" " ++ (T.unpack packedline)
      numbered_plan = map numline zpd
      in "------\n" ++  name ++ "\n" ++ (intercalate "\n" numbered_plan) ++ "\n------\n"

toParseTestCase :: (String, String) -> TestTree
toParseTestCase (a, b)  =
  let plainName  = makeTestName a b
      prs = fromString b
      detailedName = "Parse: " ++ plainName ++ groom prs ++ "\n\n"
      in testCase detailedName $ (isRight prs) @? (groom prs)

parseTestFileContents :: String -> [(String, String)]
parseTestFileContents s =
  {- the first element after split is an empty string, because we start with a plan, so must do tail -}
  let rawPairs = case (T.splitOn (T.pack "--TEST--") (T.pack s)) of
                    [] -> []
                    _ : rest -> rest
      toPairs [x,y] = (T.unpack . T.strip $ x, T.unpack . T.strip $ y)
      in map (toPairs . (T.splitOn (T.pack ";"))) rawPairs

makeParseTestTree :: String  -> [TestTree]
makeParseTestTree contents =
  let prs = parseTestFileContents contents
      in map toParseTestCase prs


-- makeMplanTestTree :: (String, String) -> [TestTree]
-- makeMplanTestTree (a

-- unitTests = testGroup "Unit tests"
--   [ -- testCase "List comparison (different length)" $
--   --     [1, 2, 3] `compare` [1,2] @?= GT

--   -- -- the following test does not hold
--   -- , testCase "List comparison (same length)" $
--   --     [1, 2, 3] `compare` [1,2,2] @?= LT
--   ]
