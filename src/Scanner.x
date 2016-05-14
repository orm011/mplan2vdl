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
  ($white#\xC)+ ;
  "//".*  ;                     -- comment
  [0-9]+ | 0x[0-9a-fA-F]+  { \posn s -> scannedToken posn $ IntLit s}
    class
  | boolean
  | callout
  | else
  | if
  | int
  | return
  | void
  | for
  | break
  | while
  | continue { \posn s -> scannedToken posn $ Keyword s }
  \' $ord \'     { \posn s -> scannedToken posn $ CharLit $ s !! 1}
  \' \\ $esc \' {
  \posn s -> scannedToken posn $
    let {sprime = case (s !! 2) of
                   '\\' -> '\\'
                   '\'' -> '\''
                   'n' -> '\n'
                   't' -> '\t'
                   '"' -> '"'
                   }
     in CharLit sprime
  }
  false | true { \posn s -> scannedToken posn $ BooleanLit s}
  \{  { \posn _ -> scannedToken posn LCurly }
  \}  { \posn _ -> scannedToken posn RCurly }
  \[  { \posn _ -> scannedToken posn LBrack }
  \]  { \posn _ -> scannedToken posn RBrack }
  \(  { \posn _ -> scannedToken posn LParen }
  \)  { \posn _ -> scannedToken posn RParen }
  \,   { \posn _ -> scannedToken posn Comma }
  \:   { \posn _ -> scannedToken posn Colon }
  \;   { \posn _ -> scannedToken posn Semi }
  \@   { \posn _ -> scannedToken posn At }
  \?   { \posn _ -> scannedToken posn Question }
  \"( \\ $esc  | $ord )* \" { \posn s -> scannedToken posn $ StringLit s }
  ($alpha|_)($alpha|_|[0-9])* { \posn s -> scannedToken posn $ Identifier s }
  "+"|"-"     { \posn s -> scannedToken posn $ AddOp s}
  "*"|"/"|"%" { \posn s -> scannedToken posn $ MulOp s}
  "<"|"<="
  |">"|">="   { \posn s -> scannedToken posn $ RelOp s}
  "!="|"=="   { \posn s -> scannedToken posn $ CmpOp s}
  \! { \posn _ -> scannedToken posn $ Not }
  \=|"+="|"-=" { \posn s -> scannedToken posn $ ScAssign s }
  "&&"    { \posn _ -> scannedToken posn $ And}
  "||"    { \posn _ -> scannedToken posn $ Or}




----------------------------- Representing tokens -----------------------------

{
-- | A token with position information.
data ScannedToken = ScannedToken { line :: Int
                                 , column :: Int
                                 , extractRawToken :: Token
                                 } deriving (Eq)

-- | A token.
data Token =
             Keyword String
           | Identifier String
           | CharLit Char
           | BooleanLit String
           | IntLit String
           | StringLit String
           | RelOp String
           | AddOp String
           | MulOp String
           | CmpOp String
           | And
           | Or
           | Not
           | ScAssign String
           | LCurly
           | RCurly
           | LBrack
           | RBrack
           | LParen
           | RParen
           | Comma
           | Colon
           | Semi
           | Question
           | At
           deriving (Eq)

instance Show Token where
 show ( Keyword s ) = s
 show ( Identifier s ) = "IDENTIFIER " ++ s
 show ( RelOp s ) = s
 show ( AddOp s ) = s
 show ( MulOp s ) = s
 show ( CmpOp s ) = s
 show And = "&&"
 show Or = "||"
 show Not = "!"
 show (ScAssign s) = s
 show ( BooleanLit s ) = "BOOLEANLITERAL " ++ s
 show ( IntLit s ) = "INTLITERAL " ++ s
 show ( StringLit s ) = "STRINGLITERAL " ++ s
 show LCurly = "{"
 show RCurly = "}"
 show LBrack = "["
 show RBrack = "]"
 show LParen = "("
 show RParen = ")"
 show Comma = ","
 show Colon = ":"
 show Semi = ";"
 show Question = "?"
 show At = "@"
 show ( CharLit c ) =
  let escaped_c =
       case c of
       '\n' -> "\\n"
       '\t' -> "\\t"
       '\\' -> "\\\\"
       '\"' -> "\\\""
       '\'' -> "\\'"
       any_c -> [any_c]
      in "CHARLITERAL " ++ "\'" ++ escaped_c ++ "\'"

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
