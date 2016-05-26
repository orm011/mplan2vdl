{- Main -- main entry point
Copyright (C) 2013, 2014  Benjamin Barenblat <bbaren@mit.edu>

This file is a part of decafc.

decafc is free software: you can redistribute it and/or modify it under the
terms of the MIT (X11) License as described in the LICENSE file.

decafc is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the X11 license for more details. -}
module MainFuns (main) where

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

import Data.List.Utils (replace)

import qualified CLI
import Configuration (Configuration, CompilerStage(..))
import qualified Configuration
import qualified Vlite as V

------------------------ Impure code: Fun with ExceptT ------------------------

(|>) :: a -> (a -> b) -> b
(|>) v f = f v

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
    input <- readFile $ Configuration.input configuration
    -- Part II: Process it
    hoistEither $ V.fromString input
  case result of
    -- Part III: Write output
    Left errorMessage -> fatal errorMessage
    Right vl -> putStrLn $ groom vl
  where hoistEither = ExceptT . return

readFile :: FilePath -> ExceptT String IO String
readFile name = liftIO $ Prelude.readFile name

fatal :: String -> IO ()
fatal message = do
  progName <- getProgName
  hPutStrLn stderr $ printf "%s: %s" progName message
  System.Exit.exitFailure
