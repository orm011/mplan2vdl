module MainFuns (main) where

import System.Environment (getProgName)
import qualified System.Exit
import System.IO (IOMode(..), hClose, hPutStrLn, openFile, stdout, stderr)
import Text.Printf (printf)
import Text.Groom (groom)

import Data.List.Utils (startswith)
import Data.String.Utils (lstrip)

import System.Console.CmdArgs.Implicit
import Config

--compiler stages
import qualified Parser as P -- raw parse tree.
import qualified Mplan as M -- parse tree.knows about differences between operators.
import qualified Vlite as Vl -- light vector language similar to voodoo.
import qualified Voodoo as Vd -- for pretty printing ./Driver readable code

cmdTemplate = Config
  { mplanfile = def &= args &= typ "FILE"
  , grainsize = def &= opt "8192" &= typ "INT" &= help "Grain size for foldSum/foldMax/etc"
  }
  &= summary "Mplan2Vdl transforms monetDB logical plans to voodoo"

main :: IO ()
main = do
  config <- cmdArgs cmdTemplate
  if mplanfile config == []
    then (hPutStrLn stderr (printf "Usage: need an input filename (see --help)")
         >> System.Exit.exitFailure)
    else return  ()
  contents <- readFile $ mplanfile config
  let lins = lines contents
  let ignore ln = ( startswith "#" stripped) || ( startswith "%" stripped)
        where stripped = lstrip ln
  let justThePlan = concat $ filter (not . ignore) lins
  let res = do parseTree <- P.fromString justThePlan config
               mplan <- M.mplanFromParseTree parseTree config
               vexps <- Vl.vexpsFromMplan mplan config
               vdl <- Vd.vdlFromVexps vexps config
               return $ vdl
  case res of
    Left errorMessage -> fatal errorMessage
    Right result -> putStrLn $ show result

fatal :: String -> IO ()
fatal message = do
  progName <- getProgName
  hPutStrLn stderr $ printf "%s: %s" progName message
  System.Exit.exitFailure

-- fromFile :: String -> IO (Either String [(Int, V.Vref)])
-- fromFile f =
--   do input <- readFile f
--      let sol = V.fromString input
--      return sol

{-
fromString :: String -> Either String RelExpr
fromString mplanstring =
  do parsetree <- P.fromString mplanstring
     let mplan = (fromParseTree $!! parsetree) >>= (return . fuseSelects . pushFKJoins)
     -- let tr = case mplan of
     --            Left err -> "\n--Error at Mplan stage:\n" ++ err
     --            Right g -> "\n--Mplan output:\n" ++ groom g
     -- trace tr mplan
     mplan

-- string means monet plan string.
fromString :: String -> Either String [(Vexp, Maybe Name)]
fromString mplanstring =
  do mplan <- M.fromString mplanstring
     let vlite = fromMplan $!! mplan
     -- let tr = case vlite of
     --            Left err -> "\n--Error at Vlite stage:\n" ++ err
     --            Right g -> "\n--Vlite output:\n" ++ groom g
     -- trace tr vlite
     vlite

-}
