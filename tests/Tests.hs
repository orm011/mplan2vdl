--import Test.Tasty
{- import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC -}
-- import Test.Tasty.HUnit
-- import Data.Either(isRight)
-- import qualified Data.Text as T
-- --import Text.Groom
-- import Debug.Trace
-- import Data.List(intercalate)

--Import qualified Parser as P
--import qualified Mplan as M
--import qualified Vlite as V

--import Control.DeepSeq(NFData)

main :: IO ()
main = putStrLn "runs nothing"

  -- do adhoc <- readFile "tests/ad_hoc_tests.txt"
  --         tpch <- readFile "tests/tpch_query_plans.txt"
  --         detailed  <- readFile "tests/detailed_tests.txt"
  --         let adhoc_cases = splitFileIntoTests adhoc
  --         let tpch_cases = splitFileIntoTests tpch
  --         let detailed_cases = splitFileIntoTests detailed
  --         defaultMain $ testGroup "Tests"
--           [ -- testGroup "AdHocParseTests" $ makeTestTree "parse" P.fromString adhoc_cases,
             -- testGroup "TPCHParseTests" $ makeTestTree "parse" P.fromString tpch_cases,
             -- testGroup "DetailedParseTests" $ makeTestTree "parse" P.fromString detailed_cases,

             -- testGroup "AdHocMplanGenTests" $ makeTestTree "mplan" M.fromString  adhoc_cases,
             -- testGroup "TPCHMplanGenTests" $ makeTestTree "mplan" M.fromString tpch_cases,
             -- testGroup "AdHocVliteGenTests" $ makeTestTree "vlite" V.fromString adhoc_cases,
             -- testGroup "TPCHVliteGenTests" $ makeTestTree "vlite" V.fromString tpch_cases,
           --   testGroup "end" []
           -- ]

-- makeTestName :: String ->  String -> String
-- makeTestName name plantext =
--   let zpd = zip [1..] (T.splitOn (T.pack "\n") (T.pack plantext))
--       numline (num, packedline) = (show num) ++ (if num < 10 then " " else "") ++" " ++ (T.unpack packedline)
--       numbered_plan = map numline zpd
--       in "------\n" ++  name ++ "\n" ++ (intercalate "\n" numbered_plan) ++ "\n------\n"

-- splitFileIntoTests :: String -> [(String, String)]
-- splitFileIntoTests s =
--   {- the first element after split is an empty string, because we start with a plan, so must do tail -}
--   let rawPairs = case (T.splitOn (T.pack "--TEST--") (T.pack s)) of
--                     [] -> []
--                     _ : rest -> rest
--       toPairs [x,y] = (T.unpack . T.strip $ x, T.unpack . T.strip $ y)
--       in map (toPairs . (T.splitOn (T.pack ";"))) rawPairs

-- makeTestTree :: (Show a, NFData a) => String -> (String -> Either String a) -> [(String,String)] -> [TestTree]
-- makeTestTree compilername compiler  pairs   = map helper pairs
--   where helper (a, b)  = let plainName  = makeTestName a b
--                              forlog = "\n--TEST--\n--SQL:\n" ++ a ++ "\n\n--MonetPlanString:\n" ++ b
--                              prs = compiler (trace forlog b)
--                              detailedName = compilername ++ plainName ++ "\n\n"
--                              msg = "failed " ++ compilername  ++ " test"
--                              tc = testCase detailedName $ (isRight prs) @? msg
--                              in localOption (mkTimeout 1000000) {-100 milliseconds-} tc


