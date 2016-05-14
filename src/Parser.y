-- Parser -- Decaf parser                                       -*- haskell -*-
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
module Parser ( parse
              ) where

import Text.Printf (printf)

import Scanner (ScannedToken(..), Token(..))

}


--------------------------------- Directives ----------------------------------

%name parse
%error { parseError }
%monad { Either String }

%tokentype { ScannedToken }

%token
  identifier { ScannedToken _ _ (Identifier $$) }
  '{'        { ScannedToken _ _ LCurly }
  '}'        { ScannedToken _ _ RCurly }
  IntType    { ScannedToken _ _ (Keyword "int") }
  BoolType   { ScannedToken _ _ (Keyword "boolean") }
  ';'        { ScannedToken _ _ Semi }
  '['        { ScannedToken _ _ LBrack }
  ']'        { ScannedToken _ _ RBrack }
  intliteral { ScannedToken _ _ (IntLit $$) }
  booleanliteral { ScannedToken _ _ (BooleanLit $$) }
  charliteral { ScannedToken _ _ (CharLit $$)}
  stringliteral   { ScannedToken _ _ (StringLit $$ )}
  void       { ScannedToken _ _ (Keyword "void")}
  '('        { ScannedToken _ _ LParen }
  ')'        { ScannedToken _ _ RParen }
  '='        { ScannedToken _ _ (ScAssign "=") }
  '+='       { ScannedToken _ _ (ScAssign "+=") }
  '-='       { ScannedToken _ _ (ScAssign "-=") }
  return     { ScannedToken _ _ (Keyword "return") }
  break      { ScannedToken _ _ (Keyword "break") }
  continue   { ScannedToken _ _ (Keyword "continue") }
  callout    { ScannedToken _ _ (Keyword "callout") }
  ','        { ScannedToken _ _ Comma }
  Plus       { ScannedToken _ _ (AddOp "+") }
  Minus      { ScannedToken _ _ (AddOp "-") }
  '!'        { ScannedToken _ _ Not }
  '@'         { ScannedToken _ _ At }
  MulOp      { ScannedToken _ _ (MulOp $$) }
  if         { ScannedToken _ _ (Keyword "if") }
  else       { ScannedToken _ _ (Keyword "else") }
  for       { ScannedToken _ _ (Keyword "for") }
  relOp     { ScannedToken _ _ (RelOp $$ ) }
  cmpOp     { ScannedToken _ _ (CmpOp $$ ) }
  while     { ScannedToken _ _ (Keyword "while") }
  and        { ScannedToken _ _ And }
  or        { ScannedToken _ _ Or }
  '?'       { ScannedToken _ _ Question }
  ':'       { ScannedToken _ _ Colon }

%% -------------------------------- Grammar -----------------------------------

Program
: CalloutList ProgramRec { let (f, m) :: ([Field], [Method]) = $2 in Program { callouts=$1, fields=f, methods=m } }

ProgramRec {- this helper is needed to solve a shift/reduce conflict that
happens if we simply say program = field* method*, that makes the parser
assume a method is actually a field based on how it starts?, and then fails when it bumps into the first Lparen.  -}
: FieldLine ProgramRec { let (a :: [Field], b :: [Method]) = $2 in ((($1::[Field]) ++ a), b) }
| MethodList { ([], $1) }

Callout
: callout identifier ';' { Callout { callout_id=$2 } }

CalloutList
: { [] }
| Callout CalloutList { $1 : $2 }

MethodList
: { [] }
| Method MethodList { $1 : $2 }

Method
: MethField '(' ParamList ')' Block  {
  Method { method_return = Some (fst $1), method_id = (snd $1), method_params=$3, method_block=$5 }
}
| void identifier '(' ParamList ')' Block  {
  Method { method_return = Void, method_id = $2, method_params=$4, method_block=$6 }
}

FieldList
: { [] }
| FieldLine FieldList  { ($1 ++ $2) ::[Field] } {- each line is itself a list. -}

FieldLine
: Type FieldIdList ';' {
  (let (field_tp :: Type) = $1 in
       let f ((e, Nothing) :: (String, Maybe String)) = Scalar { field_tp=field_tp, field_id=e }
           f (e, Just sz) = Array { field_tp=field_tp, field_id=e, array_sz=sz }
       in (map f (( $2 ):: [ (String, Maybe String)]))) :: [Field]
  }

FieldIdList
: BasicField { [ $1 ] }
| BasicField ',' FieldIdList { $1 : $3 }

BasicField
: identifier { ($1, Nothing) }
| identifier '[' intliteral ']' { ($1, Just $3) }

MethField {- probably not needed. it was an attempt to fix some conflict -}
: Type identifier { ($1, $2) }

Type
: IntType  { IntType }
| BoolType { BoolType }

ParamList
: ParamListNE { $1 }
| { [] }

ParamListNE
: MethField { [$1] }
| MethField ',' ParamListNE { $1 : $3 }

ArgList
: { [] }
| ArgListNE { $1 }

ArgListNE
: Expr { [$1] }
| Expr ',' ArgListNE { $1 : $3 }

Statement
: Lvalue '=' Expr ';'  { Assign { assign_op=Plain, assign_lvalue=$1, assign_rvalue=$3 } }
| Lvalue '+=' Expr ';' { Assign { assign_op=AddEq, assign_lvalue=$1, assign_rvalue=$3 } }
| Lvalue '-=' Expr ';' { Assign { assign_op=SubEq, assign_lvalue=$1, assign_rvalue=$3 }}
| return Expr ';' { Return { return_value=Just $2 }  }
| return ';' { Return { return_value=Nothing }}
| break ';'  { Break }
| continue ';' { Continue }
| identifier '(' ArgList ')' ';' {
    CallStmt $ Call { call_id=$1, call_args=$3 }
    }
| if '(' Expr ')' Block { If { if_cond=$3, if_body=$5,  if_else=Nothing } }
| if '(' Expr ')' Block else Block { If { if_cond=$3, if_body=$5, if_else=Just $7 }  }
| for '(' identifier '=' Expr ',' Expr ')' Block
{
  For { for_start_id=$3
      , for_start_val=$5
      , for_term=$7
      , for_step=Nothing
      }

  }
| for '(' identifier '=' Expr ',' Expr  ',' intliteral ')' Block
{
  For { for_start_id=$3
      , for_start_val=$5
      , for_term=$7
      , for_step=Just $ IntLiteral $9
      }
  }
| while '(' Expr ')' Block {
    While { while_cond = $3
          , while_body = $5
          }
    }


StatementList
:  { [] }
| Statement StatementList { $1 : $2 }

Block
: '{' FieldList StatementList '}' { Block { block_fields=$2, block_statements=$3}  }

Expr
: Ternary { $1 }

Ternary
: OrExpr { $1 }
| OrExpr '?' Ternary ':' Ternary {
    Ternary { ter_cond=$1
            , ter_true=$3
            , ter_false=$5
            }
    }


OrExpr
: AndExpr { $1 }
| AndExpr or OrExpr  { Bool { bop=BOr, aleft=$1, aright=$3 }  }

AndExpr
: CmpExpr { $1 }
| CmpExpr and AndExpr  { Bool { bop=BAnd, aleft=$1, aright=$3 }  }

CmpExpr
: RelExpr { $1}
| RelExpr cmpOp CmpExpr { Cmp { cop=case $2 of
                                      "==" -> CmpEq
                                      "!=" -> CmpNeq
                              , aleft=$1
                              , aright=$3 }  }

RelExpr
: AddExpr { $1}
| AddExpr relOp AddExpr { Rel { rop=case $2 of
                                     ">=" -> RelGeq
                                     ">" -> RelGt
                                     "<" -> RelLt
                                     "<=" -> RelLeq
                              , aleft=$1
                              , aright=$3 } }



AddExpr
: MulExpr { $1 }
| AddExpr Plus MulExpr  {
    Arith { aop=Add
          , aleft = $1
          , aright = $3
          }
    }
| AddExpr Minus MulExpr {
    Arith { aop=Sub
          , aleft = $1
          , aright = $3
          }
    }

MulExpr
: UnaryExpr { $1 }
| MulExpr MulOp UnaryExpr {
    Arith { aop = case $2 of
              "*" -> Mul
              "/" -> Div
              "%" -> Mod
          , aleft = $1
          , aright = $3 }
    }

UnaryExpr
: SimpleExpr { $1 }
| Minus UnaryExpr {
    Unary { uop=UMinus
          , udata=$2 }
    }
|'@' UnaryExpr {
   Unary { uop=UAt
         , udata=$2 }
   }
|'!' UnaryExpr {
    Unary { uop=UNot
          , udata=$2 }
    }

SimpleExpr
: intliteral { Lit $ IntLiteral $1 }
| booleanliteral { Lit $ BooleanLiteral $1 }
| stringliteral { Lit $ StringLiteral $1 }
| charliteral { Lit $ CharLiteral $1 }
| identifier '(' ArgList  ')' { CallExpr $ Call { call_id=$1, call_args=$3 } }
| Lvalue { Location $1 }
| '(' Expr ')' { $2 }

Lvalue
: identifier { ScalarLvalue $1 }
| identifier '[' Expr ']' { ArrayLvalue {arrayloc_id=$1, arrayloc_pos=$3 } }


----------------------------------- Haskell -----------------------------------
{
data Program = Program  { callouts:: [Callout], fields :: [Field], methods :: [Method]  } deriving (Eq)

data Field = Scalar { field_tp :: Type
                    , field_id :: String }
            | Array { field_tp :: Type
                    , field_id :: String
                    , array_sz :: String } deriving (Eq)

data Callout = Callout { callout_id:: String } deriving (Eq)

data Type = IntType | BoolType deriving (Eq)

data ReturnType = Some Type | Void deriving (Eq)

data Method =
  Method
  { method_return  :: ReturnType
  , method_id :: String
  , method_params :: [(Type, String)]
  , method_block :: Block
  } deriving (Eq)

data Block =
  Block
  { block_fields :: [Field]
  , block_statements :: [Statement] } deriving (Eq)

data Literal =
  IntLiteral String
  | BooleanLiteral String
  | StringLiteral String
  | CharLiteral Char deriving (Eq)

data Call = Call {
  call_id::String
  , call_args::[Expr]
  } deriving (Eq)

data Expr =
  Lit Literal
  | Location Lvalue
  | CallExpr Call
  | Arith { aop::ArithOp, aleft::Expr, aright::Expr }
  | Cmp { cop::CmpOp, aleft::Expr, aright::Expr }
  | Rel { rop::RelOp, aleft::Expr, aright::Expr }
  | Bool { bop::BoolOp, aleft::Expr, aright::Expr }
  | Unary { uop::UnaryOp, udata::Expr}
  | Ternary { ter_cond::Expr, ter_true::Expr, ter_false::Expr }
  deriving (Eq)

data ArithOp =
  Add
  | Sub
  | Mul
  | Div
  | Mod deriving (Eq)

data UnaryOp =
  UMinus
  | UNot
  | UAt deriving (Eq)

data BoolOp =
  BAnd
  | BOr deriving (Eq)

{- boolean,boolean or int,int input type, boolean output type -}
data CmpOp =
  CmpEq
  | CmpNeq deriving (Eq)

{- integer input type, boolean output type -}
data RelOp =
  RelLt
  | RelLeq
  | RelGt
  | RelGeq deriving (Eq)

data Lvalue =
  ScalarLvalue String
  | ArrayLvalue
    { arrayloc_id :: String
    , arrayloc_pos :: Expr } deriving (Eq)

data AssignOp =
  Plain
  | AddEq
  | SubEq deriving (Eq)

data Statement =
  Assign
  { assign_op :: AssignOp
  , assign_lvalue :: Lvalue
  , assign_rvalue :: Expr
  }
  | Return
    { return_value :: Maybe Expr }
  | Break
  | Continue
  | If
    { if_cond::Expr
    , if_body::Block
    , if_else::Maybe Block }
  | For { for_start_id :: String
        , for_start_val :: Expr
        , for_term :: Expr
        , for_step :: Maybe Literal }
  | While { while_cond :: Expr, while_body :: Block }
  | CallStmt Call deriving (Eq)

instance Show Type where
  show IntType = "IntType"
  show BoolType = "BoolType"


parseError :: [ScannedToken] -> Either String a
parseError [] = Left "unexpected EOF"
parseError toks =
  Left $ printf "line %d:%d: unexpected token%s '%s'."
                lineNo
                columnNo
                (if (not $ null $ tail toks) then "s" else "")
                badTokenText
  where firstBadToken = head toks
        lineNo = Scanner.line firstBadToken
        columnNo = Scanner.column firstBadToken
        badTokenText = concatMap (show . extractRawToken) toks
}
