module MainFuns (main) where

import System.Environment (getProgName,getArgs)
import qualified System.Exit
import System.IO (hPutStrLn, stderr)
import Text.Printf (printf)
--import Text.Groom (groom)
--import Control.Monad(foldM)
--import Data.List.Utils (startswith)
--import Data.String.Utils (lstrip,join)
import System.Console.CmdArgs.Implicit
import Config

--import Debug.Trace
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
--import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Dot

data Mplan2Vdl =  Mplan2Vdl { mplanfile :: String
                            , grainsize :: Int
                            , boundsfile :: String
                            , schemafile :: String
                            , dot :: Bool
                            , apply_cleanup_passes::Bool
                            , push_joins :: Bool
                            , shuffle_aggs_flag :: Bool
                            } deriving (Show, Data, Typeable)

cmdTemplate :: Mplan2Vdl
cmdTemplate = Mplan2Vdl
  { mplanfile = def &= args &= typ "FILE"
  , grainsize = 8192 &= typ "POWER OF 2" &= help "Grain size for foldSum/foldMax/etc (default 8192)" &= name "g"
  , boundsfile = def &= typ "CSV FILE" &= help "file in (table,col,min,max,count) csv format" &= name "b"
  , schemafile = def &= typ "msqldump file" &= help "output of msqldump -D -d <dbname>" &= name "s"
  , dot = False &= typ "BOOL" &= help "instead of running compiler, emit dot for monet plan" &= name "d"
  , push_joins = False &= typ "Bool" &= help "push joins below selects, and merges those selects when possible" &= name "p"
  , apply_cleanup_passes = True &= typ "BOOL" &= help "after generating vdl identify and clean up known no-op patterns" &= name "c"
  , shuffle_aggs_flag = False &= typ "BOOL" &= help "insert shuffle operator in aggregates" &= name "shuffle"
  }
  &= summary "Mplan2Vdl transforms monetDB logical plans to voodoo"
  &= program "mplan2vdl"

checkInput :: String -> Bool -> IO ()
checkInput msg f  = if f
                    then return  ()
                    else (hPutStrLn stderr $ "usage: " ++ msg ++ " (see --help)")
                         >> System.Exit.exitFailure

iscomment :: B.ByteString -> Bool
iscomment ln = let stripped = C.dropWhile (== ' ') ln
               in (C.isPrefixOf "#" stripped) || ( C.isPrefixOf "%" stripped)
                  || ( C.isPrefixOf "--" stripped)

filterComments :: B.ByteString -> B.ByteString
filterComments alltext =
  let lins = C.lines alltext
  in C.intercalate "\n" $ map (\l -> if iscomment l then "" else l) lins
     -- unlike filter, replacing with empty string preserves line numbers

readCommentedFile :: String -> IO B.ByteString
readCommentedFile fname  = do contents <- B.readFile fname
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
  do boundsf <- B.readFile fname
     return $ ( decode NoHeader boundsf)

main :: IO ()
main = do
  gargs <-  getArgs
  hPutStrLn stderr (show gargs)
  cmdargs <- cmdArgs cmdTemplate
  checkUsage cmdargs
  let action = if dot cmdargs
               then emitdot $ mplanfile cmdargs
               else (compile (apply_cleanup_passes cmdargs) (push_joins cmdargs))
  let grainsizelg = fromInteger $ toInteger $ countTrailingZeros $ grainsize cmdargs
  monetplan <- readCommentedFile $ mplanfile cmdargs
  monetschema <- readCommentedFile $ schemafile cmdargs
  mboundslist <- readBoundsFile $ boundsfile cmdargs
  let res = (do boundslist <- mboundslist -- maybe monad
                tables <- SP.fromString monetschema
                config <- makeConfig grainsizelg boundslist (shuffle_aggs_flag cmdargs) tables
                action monetplan config)
  case res of
    Left errorMessage -> fatal errorMessage
    Right result -> C.putStrLn $ result

fatal :: String -> IO ()
fatal message = do
  progName <- getProgName
  hPutStrLn stderr $ printf "%s: %s" progName message
  System.Exit.exitFailure

emitdot :: String -> C.ByteString -> Config -> Either String C.ByteString
emitdot qname planstring config =
  do parseTree <- case TP.fromString planstring config of
       Left err -> Left $ "(at Parse stage)" ++ err
       other -> other
     return $ Dot.toDotString (C.pack qname) parseTree

compile :: Bool -> Bool -> C.ByteString -> Config -> Either String C.ByteString
compile apply_passes push_fk_joins planstring config =
  do parseTree <- case P.fromString planstring config of
                    Left err -> Left $ "(at Parse stage)" ++ err
                    other -> other
     mplan <- case M.mplanFromParseTree parseTree config of
                  Left err -> Left $ "(at Mplan stage)" ++ err
                  other -> other
     --apply logical plan transforms here
     let rel_passes = if push_fk_joins then
                      (M.fuseSelects . M.pushFKJoins)
                      else (\x -> x)
     let mplan' = rel_passes mplan
     vexps <- case Vl.vexpsFromMplan mplan' config of
                  Left err -> Left $ "(at Vlite stage)" ++ err
                  other -> other
     let passes = if apply_passes then
                   (Vl.algebraicIdentitiesPass . Vl.loweringPass . Vl.redundantRangePass)  else (\x -> x)
     let vexps' =  passes vexps
     vdl <- case Vdl.vdlFromVexps vexps' of
                  Left err -> Left $ "(at Vdl stage)" ++ err
                  other -> other
     return $ (C.pack $ show vdl)
