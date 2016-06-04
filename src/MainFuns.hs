module MainFuns (main) where

import System.Environment (getProgName)
import qualified System.Exit
import System.IO (hPutStrLn, stderr)
import Text.Printf (printf)
--import Text.Groom (groom)

import Data.List.Utils (startswith)
import Data.String.Utils (lstrip)

import System.Console.CmdArgs.Implicit
import Config

--compiler stages
import qualified Parser as P -- raw parse tree.
import qualified Mplan as M -- monet relational plan
import qualified Vlite as Vl -- voodoo like language
import qualified Vdl -- for pretty printing ./Driver readable code


cmdTemplate :: Config
cmdTemplate = Config
  { mplanfile = def &= args &= typ "FILE"
  , grainsize = def &= opt "8192" &= typ "INT" &= help "Grain size for foldSum/foldMax/etc"
  }
  &= summary "Mplan2Vdl transforms monetDB logical plans to voodoo"

main :: IO ()
main = do
  config <- cmdArgs cmdTemplate
  if mplanfile config == []
    then (hPutStrLn stderr "usage: need an input filename (see --help)")
         >> System.Exit.exitFailure
    else return  ()
  contents <- readFile $ mplanfile config
  let lins = lines contents
  let iscomment ln = (startswith "#" stripped) || ( startswith "%" stripped)
        where stripped = lstrip ln
  let cleanPlan = concat $ filter (not . iscomment) lins
  case compile cleanPlan config of
    Left errorMessage -> fatal errorMessage
    Right result -> putStrLn $ result

fatal :: String -> IO ()
fatal message = do
  progName <- getProgName
  hPutStrLn stderr $ printf "%s: %s" progName message
  System.Exit.exitFailure

compile :: String -> Config -> Either String String
compile planstring config =
  do parseTree <- case P.fromString planstring config of
                    Left err -> Left $ "(at Parse stage)" ++ err
                    other -> other
     mplan <- case M.mplanFromParseTree parseTree config of
                  Left err -> Left $ "(at Mplan stage)" ++ err
                  other -> other
     --apply logical plan transforms here
     let mplan' = (M.fuseSelects . M.pushFKJoins) mplan
     vexps <- case Vl.vexpsFromMplan mplan' config of
                  Left err -> Left $ "(at Vlite stage)" ++ err
                  other -> other
     vdl <- case Vdl.vdlFromVexps vexps config of
                  Left err -> Left $ "(at Vdl stage)" ++ err
                  other -> other
     return $ show vdl
