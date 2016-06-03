module MainFuns (main, fromFile) where

import System.Environment (getProgName)
import qualified System.Exit
import System.IO (IOMode(..), hClose, hPutStrLn, openFile, stdout, stderr)
import Text.Printf (printf)
import Text.Groom (groom)

import Data.List.Utils (startswith)
import Data.String.Utils (lstrip)

import qualified Voodoo as V
import System.Console.CmdArgs.Implicit

------------------------ Impure code: Fun with ExceptT ------------------------

data Mplan2Vdl = Mplan2Vdl  {mplanfile :: String
                           , groupwidths :: Int
                           } deriving (Show, Data, Typeable)

cmdTemplate = Mplan2Vdl
  { mplanfile = def &= args &= typ "FILE"
  , groupwidths = def &= opt "8192" &= typ "INT" &= help "Grain size for foldSum/foldMax/etc"
  }
  &= summary "Mplan2Vdl transforms monetDB logical plans to voodoo"

main :: IO ()
main = do
  args <- cmdArgs cmdTemplate
  if mplanfile args == []
    then (hPutStrLn stderr (printf "Usage: need an input filename (see --help)")
         >> System.Exit.exitFailure)
    else return  ()
  contents <- readFile $ mplanfile args
  let lins = lines contents
  let ignore ln = ( startswith "#" stripped) || ( startswith "%" stripped)
        where stripped = lstrip ln
  let justThePlan = concat $ filter (not . ignore) lins
  let res = V.vdlFromMplan justThePlan
  case res of
    Left errorMessage -> fatal errorMessage
    Right vl -> putStrLn $ vl

fatal :: String -> IO ()
fatal message = do
  progName <- getProgName
  hPutStrLn stderr $ printf "%s: %s" progName message
  System.Exit.exitFailure

fromFile :: String -> IO (Either String [(Int, V.Vref)])
fromFile f =
  do input <- readFile f
     let sol = V.fromString input
     return sol
