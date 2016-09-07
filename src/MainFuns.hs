module MainFuns (main) where

import Control.Monad.Reader (runReader)
import System.Environment (getProgName,getArgs)
import qualified System.Exit
import System.IO (hPutStrLn, stderr, stdin)
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
                            , boundsfile :: String
                            , storagefile :: String
                            , schemafile :: String
                            , dictionaryfile :: String
                            , metadata :: Bool
                            , dot :: Bool
                            , apply_cleanup_passes::Bool
                            , push_joins :: Bool
                            , agg_strategy :: AggStrategy
                            , grainsize :: Int
                            } deriving (Show, Data, Typeable)

cmdTemplate :: Mplan2Vdl
cmdTemplate = Mplan2Vdl
  { mplanfile = def &= args &= typ "FILE"
  , boundsfile = def &= typ "CSV FILE" &= help "file in (table,col,min,max,count) csv format" &= name "b"
  , storagefile = def &= typ "CSV FILE" &= help "output of 'select * from storage' in csv format" &= name "t"
  , schemafile = def &= typ "msqldump file" &= help "output of msqldump -D -d <dbname>" &= name "s"
  , dictionaryfile = def &= typ "CSV FILE" &= help "dictonary to encode literal strings" &= name "dictionary"
  , dot = False &= typ "BOOL" &= help "instead of running compiler, emit dot for monet plan" &= name "d"
  , push_joins = False &= typ "Bool" &= help "push joins below selects, and merges those selects when possible" &= name "p"
  , apply_cleanup_passes = True &= typ "BOOL" &= help "after generating vdl identify and clean up known no-op patterns" &= name "c"
  , agg_strategy = enum
        [AggSerial &= help "serial aggregation"
        ,AggHierarchical 13 &= help "2-level hierarchical aggregation. use together with the grain size argument."
        ,AggShuffle &= help "parallel agg with shuffle operator"]
  , grainsize = 8192 &= typ "POWER OF 2" &= help "Grain size for --agghierarchical (default 8192). Ignored otherwise" &= name "g"
  , metadata = False &= typ "Bool" &= help "show inferred metadata in output"
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

readCommentedFileFromStdIn :: IO B.ByteString
readCommentedFileFromStdIn = do contents <- B.hGetContents stdin
                                return $ filterComments contents

checkUsage :: Mplan2Vdl -> IO ()
checkUsage cmdargs  =
  do if (dot cmdargs)
       then return ()
       else
       do checkInput "need a column bounds csv" $ boundsfile cmdargs /= []
          checkInput "need a schema file"  $ schemafile cmdargs /= []
          checkInput "need a storage file" $ storagefile cmdargs /= []
          checkInput "need a dictionary file" $ dictionaryfile cmdargs /= []
          let mgrainsize = grainsize cmdargs
          checkInput "grainsize must be a power of 2" $ (mgrainsize  >= 0) && (popCount mgrainsize  == 1)

readBoundsFile :: String -> IO (Either String (V.Vector BoundsRec))
readBoundsFile fname =
  do boundsf <- B.readFile fname
     return $ ( decode NoHeader boundsf)

readStorageFile :: String -> IO (Either String (V.Vector StorageRec))
readStorageFile fname =
  do storagef <- B.readFile fname
     return $ ( decode NoHeader storagef)

readDictionaryFile :: String -> IO (Either String (V.Vector DictRec))
readDictionaryFile dname =
  do dictf <- B.readFile dname
     return $ ( decode NoHeader dictf)


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
  monetplan <- if mplanfile cmdargs /= [] then readCommentedFile (mplanfile cmdargs) else hPutStrLn stderr "reading from stdin" >> readCommentedFileFromStdIn
  monetschema <- readCommentedFile $ schemafile cmdargs
  mboundslist <- readBoundsFile $ boundsfile cmdargs
  mstoragelist <- readStorageFile $ storagefile cmdargs
  mdictlist <- readDictionaryFile $ dictionaryfile cmdargs
  let strat = case agg_strategy cmdargs of
        AggHierarchical _ -> AggHierarchical (grainsizelg)
        x -> x
  let res = (do boundslist <- mboundslist -- maybe monad
                tables <- SP.fromString monetschema
                storagelist <- mstoragelist
                dictlist <- mdictlist
                config <- makeConfig (metadata cmdargs) strat boundslist storagelist tables dictlist
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
     let vdl = runReader (Vdl.vdlFromVexps vexps') config
     return $ (C.pack $ show vdl)
