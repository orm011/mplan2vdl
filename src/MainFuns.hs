module MainFuns (main) where

import System.Environment (getProgName)
import qualified System.Exit
import System.IO (hPutStrLn, stderr)
import Text.Printf (printf)
--import Text.Groom (groom)
--import Control.Monad(foldM)
import Data.List.Utils (startswith)
import Data.String.Utils (lstrip,join)
import System.Console.CmdArgs.Implicit
import Config

--compiler stages
import qualified Parser as P -- raw parse tree.
import qualified SchemaParser as SP
import qualified TreeParser as TP -- for dot
import qualified Mplan as M -- monet relational plan
import qualified Vlite as Vl -- voodoo like language
import qualified Vdl -- for pretty printing ./Driver readable code
import Data.Bits
import Data.Csv
import Data.Int()
import qualified Data.Vector as V
import Data.Vector()
import Name as NameTable()
import qualified Data.ByteString.Lazy as BL
import qualified Dot

data Mplan2Vdl =  Mplan2Vdl { mplanfile :: String
                            , grainsize :: Int
                            , boundsfile :: String
                            , schemafile :: String
                            , dot :: Bool
                            } deriving (Show, Data, Typeable)

cmdTemplate :: Mplan2Vdl
cmdTemplate = Mplan2Vdl
  { mplanfile = def &= args &= typ "FILE"
  , grainsize = 8192 &= typ "POWER OF 2" &= help "Grain size for foldSum/foldMax/etc (default 8192)" &= name "g"
  , boundsfile = def &= typ "CSV FILE" &= help "file in (table,col,min,max,count) csv format" &= name "b"
  , schemafile = def &= typ "msqldump file" &= help "schema for database. via msqldump -d"
  , dot = False &= typ "BOOL" &= help "instead of running compiler, emit dot for monet plan" &= name "d"
  }
  &= summary "Mplan2Vdl transforms monetDB logical plans to voodoo"
  &= program "mplan2vdl"

checkInput :: String -> Bool -> IO ()
checkInput msg f  = if f
                    then return  ()
                    else (hPutStrLn stderr $ "usage: " ++ msg ++ " (see --help)")
                         >> System.Exit.exitFailure

iscomment :: String -> Bool
iscomment ln = let stripped = lstrip ln
               in (startswith "#" stripped) || ( startswith "%" stripped)
                  || ( startswith "--" stripped)

filterComments :: String -> String
filterComments alltext =
  let lins = lines alltext
  in join "\n" $ map (\l -> if iscomment l then "" else l) lins
     -- unlike filter, replacing with empty string preserves line numbers

readCommentedFile :: String -> IO String
readCommentedFile fname  = do contents <- readFile fname
                              return $ filterComments contents

checkUsage :: Mplan2Vdl -> IO ()
checkUsage cmdargs  =
  do checkInput "need an input mplan" $ mplanfile cmdargs /= []
     if (dot cmdargs)
       then return ()
       else
       do checkInput "need a column bounds csv" $ boundsfile cmdargs /= []
          checkInput "need a schema file"  $ schemafile cmdargs /= []
          let mgrainsize = grainsize cmdargs
          checkInput "grainsize must be a power of 2" $ (mgrainsize  >= 0) && (popCount mgrainsize  == 1)

readBoundsFile :: String -> IO (Either String (V.Vector BoundsRec))
readBoundsFile fname =
  do boundsf <- BL.readFile fname
     return $ ( decode NoHeader boundsf)

main :: IO ()
main = do
  cmdargs <- cmdArgs cmdTemplate
  checkUsage cmdargs
  let action = if dot cmdargs
               then emitdot $ mplanfile cmdargs
               else compile
  let grainsizelg = fromInteger $ toInteger $ countTrailingZeros $ grainsize cmdargs
  monetplan <- readCommentedFile $ mplanfile cmdargs
  monetschema <- readCommentedFile $ schemafile cmdargs
  mboundslist <- readBoundsFile $ boundsfile cmdargs
  let res = (do boundslist <- mboundslist -- maybe monad
                tables <- SP.fromString monetschema
                config <- makeConfig grainsizelg boundslist tables
                action monetplan config)
  case res of
    Left errorMessage -> fatal errorMessage
    Right result -> putStrLn $ result

fatal :: String -> IO ()
fatal message = do
  progName <- getProgName
  hPutStrLn stderr $ printf "%s: %s" progName message
  System.Exit.exitFailure

emitdot :: String -> String -> Config -> Either String String
emitdot qname planstring config =
  do parseTree <- case TP.fromString planstring config of
       Left err -> Left $ "(at Parse stage)" ++ err
       other -> other
     return $ Dot.toDotString qname parseTree

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
