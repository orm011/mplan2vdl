module MainFuns (main) where

import System.Environment (getProgName)
import qualified System.Exit
import System.IO (hPutStrLn, stderr)
import Text.Printf (printf)
--import Text.Groom (groom)
import Control.Monad(foldM)
import Data.List.Utils (startswith)
import Data.String.Utils (lstrip)

import System.Console.CmdArgs.Implicit
import Config

--compiler stages
import qualified Parser as P -- raw parse tree.
import qualified Mplan as M -- monet relational plan
import qualified Vlite as Vl -- voodoo like language
import qualified Vdl -- for pretty printing ./Driver readable code
import Data.Bits
import Data.Csv
import Data.Int
import qualified Data.Vector as V
import Data.Vector()
import Name as NameTable
import qualified Data.ByteString.Lazy as BL

type BoundsRec = (String, String, Int64, Int64, Int64)

data Mplan2Vdl =  Mplan2Vdl { mplanfile :: String
                            , grainsize :: Int
                            , boundsfile :: String
                            } deriving (Show, Data, Typeable)

addEntry :: NameTable.NameTable ColInfo -> BoundsRec -> Either String (NameTable.NameTable ColInfo)
addEntry nametab (tab,col,colmin,colmax,colcount) =
  NameTable.insert (Name [tab,col]) ColInfo {bounds=(colmin, colmax), count=colcount} nametab

cmdTemplate :: Mplan2Vdl
cmdTemplate = Mplan2Vdl
  { mplanfile = def &= args &= typ "FILE"
  , grainsize = 8192 &= typ "POWER OF 2" &= help "Grain size for foldSum/foldMax/etc (default 8192)" &= name "g"
  , boundsfile = def &= typ "CSV FILE" &= help "file in (table,col,min,max,count) csv format" &= name "b"
  }
  &= summary "Mplan2Vdl transforms monetDB logical plans to voodoo"
  &= program "mplan2vdl"

main :: IO ()
main = do
  cmdargs <- cmdArgs cmdTemplate
  if mplanfile cmdargs == []
    then (hPutStrLn stderr "usage: need an input filename (see --help)")
         >> System.Exit.exitFailure
    else return  ()
  if boundsfile cmdargs == []
    then (hPutStrLn stderr "usage: need a bounds csv file (see --help)")
         >> System.Exit.exitFailure
    else return  ()
  let mgrainsize = grainsize cmdargs
  if (mgrainsize  < 0) || (popCount mgrainsize  /= 1)
    then (hPutStrLn stderr $ "usage: grainsize must be a power of 2 " ++  (show mgrainsize ) ++  (show . popCount) mgrainsize)
          >> System.Exit.exitFailure
    else return ()
  contents <- readFile $ mplanfile cmdargs
  let lins = lines contents
  let iscomment ln = (startswith "#" stripped) || ( startswith "%" stripped)
        where stripped = lstrip ln
  let cleanPlan = concat $ filter (not . iscomment) lins
  boundsf <- BL.readFile $ boundsfile cmdargs
  let mboundslist =( decode NoHeader boundsf) :: (Either String (V.Vector BoundsRec))
  let res = (do boundslist <- mboundslist
                colinfo <- foldM addEntry NameTable.empty boundslist
                let config = Config { grainsizelg = fromInteger $ toInteger $ countTrailingZeros $ grainsize cmdargs
                         , colinfo }
                compile cleanPlan config)
  case res of
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
