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
$num = [0-9]

$alnum = [$alpha $num]
$name = [$alnum \_ \%]
$withinquotes = [$name \-\ \#]

tokens :-
  ([$white \|])+ ; -- ignore vertical bars as well.
  \[  { \posn _ -> scannedToken posn LBrack }
  \]  { \posn _ -> scannedToken posn RBrack }
  \(  { \posn _ -> scannedToken posn LParen }
  \)  { \posn _ -> scannedToken posn RParen }
  \,   { \posn _ -> scannedToken posn Comma }
  \.   { \posn _ -> scannedToken posn Dot }
  \"$withinquotes* \" { \posn s -> scannedToken posn ( ValueLiteral s ) }
  $num+ {\posn s -> scannedToken posn ( NumberLiteral (read s) ) {- used only for internal types -} }
  "group by"  { \posn _ -> scannedToken posn (Word "group by") }
  "NOT NULL"  { \posn _ -> scannedToken posn (Word "NOT NULL") }
  "no nil"  { \posn _ -> scannedToken posn (Word "no nil") }
  "top N"   { \posn _ -> scannedToken posn (Word "top N") }
  "left outer join" { \posn _ -> scannedToken posn (Word "left outer join") }
  "or" { \posn _ -> scannedToken posn (Oper "or")}
  $name+ { \posn s -> scannedToken posn ( Word s ) }
  "<"|"<=" |">"|">="|"!="|"="|"!"   { \posn s -> scannedToken posn $ Oper s}


----------------------------- Representing tokens -----------------------------

{
-- | A token with position information.
data ScannedToken = ScannedToken { line :: Int
                                 , column :: Int
                                 , extractRawToken :: Token
                                 } deriving (Eq,Show)

-- | A token.
data Token =
             Word String
           | ValueLiteral String
           | NumberLiteral Int
           | LCurly
           | RCurly
           | LBrack
           | RBrack
           | LParen
           | RParen
           | Dot
           | Comma
           | Oper String
           deriving (Eq)

instance Show Token where
  show (Word s) = "(Word " ++ s ++ ")"
  show (ValueLiteral s) = "( ValueLiteral " ++ s ++ ")"
  show (NumberLiteral n) = "( NumberLitreal " ++ (show n) ++ ")"
  show LCurly = "{"
  show RCurly = "}"
  show LBrack = "["
  show RBrack = "]"
  show LParen = "("
  show RParen = ")"
  show Dot = "."
  show Comma = ","
  show (Oper s) = "(Oper " ++ s ++")"


{-  Smart constructor to create a 'ScannedToken' by extracting the line and
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
