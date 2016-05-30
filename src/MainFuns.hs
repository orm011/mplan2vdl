{- Main -- main entry point
Copyright (C) 2013, 2014  Benjamin Barenblat <bbaren@mit.edu>

This file is a part of decafc.

decafc is free software: you can redistribute it and/or modify it under the
terms of the MIT (X11) License as described in the LICENSE file.

decafc is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the X11 license for more details. -}
module MainFuns (main, fromFile) where

import Prelude hiding (readFile)
import qualified Prelude

import Control.Exception (bracket)
import Control.Monad (forM_, void)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT(..), runExceptT)
import Data.Either (partitionEithers)
import GHC.IO.Handle (hDuplicate)
import System.Environment (getProgName)
import qualified System.Exit
import System.IO (IOMode(..), hClose, hPutStrLn, openFile, stdout, stderr)
import Text.Printf (printf)
import Text.Groom (groom)

import Data.List.Utils (replace, startswith)
import Data.String.Utils (lstrip)

import qualified CLI
import Configuration (Configuration, CompilerStage(..))
import qualified Configuration
import qualified Voodoo as V
import qualified Name as Name

------------------------ Impure code: Fun with ExceptT ------------------------

main :: IO ()
main = do
  {- Compiler work can be split into three stages: reading input (impure),
  processing it (pure), and writing output (impure).  Of course, input might be
  malformed or there might be an error in processing.  Thus, it makes most
  sense to think of the compiler as having type ExceptT String IO [IO ()] --
  that is, computation might fail with a String or succeed with a series of IO
  actions. -}
  result <- runExceptT $ do
    -- Part I: Get input
    configuration <- ExceptT CLI.getConfiguration
    contents <- readFile $ Configuration.input configuration
    let lins = lines contents
    let ignore ln = let stripped = lstrip ln in
          (startswith "#" stripped) || ( startswith "%" stripped)
    let justThePlan = filter (not . ignore) lins
    -- Part II: Process it
    hoistEither $ V.vdlFromMplan $ concat justThePlan
  case result of
    -- Part III: Write output
    Left errorMessage -> fatal errorMessage
    Right vl -> putStrLn $ vl
  where hoistEither = ExceptT . return

readFile :: FilePath -> ExceptT String IO String
readFile name = liftIO $ Prelude.readFile name

fatal :: String -> IO ()
fatal message = do
  progName <- getProgName
  hPutStrLn stderr $ printf "%s: %s" progName message
  System.Exit.exitFailure

fromFile :: String -> IO (Either String [(Int, V.Vref)])
fromFile f =
  do input <- Prelude.readFile f
     let sol = V.fromString input
     return sol
