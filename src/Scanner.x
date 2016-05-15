-- Scanner -- Decaf scanner                                     -*- haskell -*-
-- Copyright (C) 2013  Benjamin Barenblat <bbaren@mit.edu>
--
-- This file is a part of decafc.
--
-- decafc is free software: you can redistribute it and/or modify it under the
-- terms of the MIT (X11) License as described in the LICENSE file.
--
-- decafc is distributed in the hope that it will be useful, but WITHOUT ANY
-- WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
-- FOR A PARTICULAR PURPOSE.  See the X11 license for more details.
{
{-# OPTIONS_GHC -w #-}
module Scanner ( ScannedToken(..)
               , Token(..)
               , scan
               , formatTokenOrError
               ) where

}

%wrapper "6.035"

----------------------------------- Tokens ------------------------------------

$alpha = [a-zA-Z]

$esc = [\\ \" \' n t]
$ord = [^ \" \\ \n \t \' \xC]

tokens :-
  ([$white \|])+ ; -- ignore vertical bars as well.
    project
  | group by
  | select
  | table
  | NOT
  | NULL
  | COUNT
  | tinyint
  | int
  | smallint
  | bigint
  | decimal
  | sum
  | sql_add
  | sql_sub
  | sql_mul
  | sql_mod
  | sql_div
  | as  { \posn s -> scannedToken posn ( Keyword s ) }
  \[  { \posn _ -> scannedToken posn LBrack }
  \]  { \posn _ -> scannedToken posn RBrack }
  \(  { \posn _ -> scannedToken posn LParen }
  \)  { \posn _ -> scannedToken posn RParen }
  \,   { \posn _ -> scannedToken posn Comma }
  \.   { \posn _ -> scannedToken posn Dot }
  \"( \\ $esc  | $ord )* \" { \posn s -> scannedToken posn ( ValueLiteral s ) }
  ($alpha|_)($alpha|_|[0-9])* { \posn s -> scannedToken posn ( Identifier s ) }
  "<"|"<="
  |">"|">="   { \posn s -> scannedToken posn $ CmpOp s}
  "!="|"="   { \posn s -> scannedToken posn $ RelOp s}


----------------------------- Representing tokens -----------------------------

{
-- | A token with position information.
data ScannedToken = ScannedToken { line :: Int
                                 , column :: Int
                                 , extractRawToken :: Token
                                 } deriving (Eq,Show)

-- | A token.
data Token =
             Keyword String
           | ValueLiteral String
           | Identifier String
           | LCurly
           | RCurly
           | LBrack
           | RBrack
           | LParen
           | RParen
           | Dot
           | Comma
           | RelOp String
           | CmpOp String
           deriving (Eq, Show)

{-| Smart constructor to create a 'ScannedToken' by extracting the line and
column numbers from an 'AlexPosn'. -}
scannedToken :: AlexPosn -> Token -> ScannedToken
scannedToken (AlexPn _ lineNo columnNo) tok = ScannedToken lineNo columnNo tok


---------------------------- Scanning entry point -----------------------------

scan :: String -> [Either String ScannedToken]
scan = alexScanTokens

formatTokenOrError :: Either String ScannedToken -> Either String String
formatTokenOrError (Left err) = Left err
formatTokenOrError (Right tok) = Right $ unwords [ show $ line tok
                                                 , show $ extractRawToken tok
                                                 ]
}
