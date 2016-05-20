module Mplan( fromParseTree
            , fromString
            , Name
            , BinaryOp
            , RelExpr(..)
            , ScalarExpr(..)) where

import qualified Parser as P
import Parser(Name)
import Control.DeepSeq(NFData)
import GHC.Generics (Generic)
import Text.Groom
import qualified Data.List.NonEmpty as Ne
import Data.String.Utils(join)
import Data.Int

data MType =
  MTinyint
  | MInt
  | MBigInt
  | MSmallint
  | MDecimal Int Int
  | MSecInterval Int
  | MMonthInterval
  | MDate
  | MCharFix Int
  | MChar
  deriving (Eq, Show, Generic)
instance NFData MType

resolveTypeSpec :: P.TypeSpec -> Either String MType
resolveTypeSpec P.TypeSpec { P.tname, P.tparams } = f tname tparams
  where f ["int"] [] = Right MInt
        f ["tinyint"] [] = Right MTinyint
        f ["smallint"] [] = Right MSmallint
        f ["bigint"] [] = Right MBigInt
        f name _ = Left $  "unsupported typespec: " ++ join "," name
        -- f ["decimal"] [a, b] = Right (MDecimal a b)
        -- f ["sec_interval"] [a] = Right (MSecInterval a)
        -- f ["month_interval"] []  = Right MMonthInterval
        -- f ["date"] []  = Right MDate
        -- f ["char"] [a] = Right (MCharFix a)
        -- f ["char"] [] = Right MChar



data OrderSpec = Asc | Desc deriving (Eq,Show, Generic)
instance NFData OrderSpec

data BinaryOp =
  Gt | Lt | Leq | Geq {- rel -}
  | Eq | Neq {- comp -}
  | LogAnd | LogOr {- logical -}
  | Sub | Add | Div | Mul | Mod | BitAnd | BitOr  {- arith -}
  deriving (Eq, Show, Generic)
instance NFData BinaryOp

resolveBinopOpcode :: Name -> Either String BinaryOp
resolveBinopOpcode nm =
  case nm of
    ["sys", "sql_add"] -> Right Add
    ["sys", "sql_sub"] -> Right Sub
    ["sys", "sql_mul"] -> Right Mul
    ["sys", "sql_div"] -> Right Div
    _ -> Left $ "unsupported binary function: " ++ join "." nm

 {- some of them are semantically for groups,
  but are syntatctically unary -}
data UnaryOp =
  Sum | Avg | Neg | Year
  deriving (Eq, Show, Generic)
instance NFData UnaryOp

resolveUnopOpcode :: Name -> Either String UnaryOp
resolveUnopOpcode nm =
  case nm of
    ["sys", "sum"] -> Right Sum
    ["sys", "avg"] -> Right Avg
    ["sys", "year"] -> Right Year
    ["sys", "sql_neg"] -> Right Neg
    _ -> Left $ "unsupported unary function " ++ join "." nm

 {- a Ref can be a column or a previously bound name for an intermediate -}
data ScalarExpr =
  Ref Name
  | IntLiteral Int64 {- use the widest possible type to not lose info -}
  | Unary { unop:: UnaryOp, arg::ScalarExpr }
  | Binop { binop :: BinaryOp, left :: ScalarExpr, right :: ScalarExpr  }
  | Cast { mtype :: MType, arg::ScalarExpr }
  deriving (Eq, Show, Generic)
instance NFData ScalarExpr

data RelExpr =
  Table       { tablename :: Name
              , tablecolumns :: [(Name, Maybe Name)]
              }
  | Project   { child :: RelExpr
              , projectout :: [(ScalarExpr, Maybe Name)]
              , order ::[(Name, OrderSpec)]
              }
  | Select    { child :: RelExpr
              , selectpredicate :: ScalarExpr
              }
  | Group     { child :: RelExpr
              , groupkeys :: [Name]
              , groupvalues :: [(ScalarExpr, Maybe Name)]
              }
  | SemiJoin  { lchild :: RelExpr
              , rchild :: RelExpr
              , condition :: ScalarExpr
              }
  | TopN
  | Cross
  | Join
  | AntiJoin
  | LeftOuter
  deriving (Eq,Show, Generic)
instance NFData RelExpr

-- thsis  way to insert extra consistency checks
check :: a -> (a -> Bool) -> String -> Either String a
check val cond msg = if cond val then Right val else Left msg


{-helper function uesd in multiple operators that introduce output
columns -}
solveOutputs :: [P.Expr] -> Either String [(ScalarExpr, Maybe Name)]
solveOutputs explist = sequence $ map f explist
  where f P.Expr { P.expr, P.alias } =
          do scalar <- sc expr
             return (scalar, alias)



solve :: P.Rel -> Either String RelExpr

{- Leaf (aka Table) invariants /checks:
  -tablecolumns must not be empty.
  -table columns must only be references, not complex expressions nor literals.
  -table columns may themselves be aliased within table.
  -some of the names involve using schema (not for now)
   for concrete resolution to things like partsupp.%partsupp_fk1
-}
solve P.Leaf { P.source, P.columns } =
  do pcols <- sequence $ map split columns
     pcols <- check pcols ( /= []) "list of table columns must not be empty"
     return $ Table { tablename = source, tablecolumns = pcols}
  where
    split P.Expr { P.expr = P.Ref { P.rname }
                 , P.alias } = Right (rname, alias)
    split _ = Left "table outputs should only have reference expressions"


{- Project invariants
- single child node
- multiple output columns with potential aliasing (non empty)
- potentially empty order columns. no aliasing there.
(what relation do they have with output ones?)
-}

solve P.Node { P.relop = "project"
             , P.children = [ch] -- only one child rel allowed for project
             , P.arg_lists = out : rest -- not dealing with order by right now.
             } =
  do child <- solve ch
     projectout <- solveOutputs out
     order  <- (case rest of
                 [] -> Right []
                 _ -> Left "not dealing with order-by clauses")
     return $ Project {child, projectout, order }


  {- Group invariants:
     - single child node
     - multiple output value columns with potential expressions (non-empty)
     - multiple group key value columns (non-empty)
  -}
solve P.Node { P.relop = "group by"
             , P.children = [ch] -- only one child rel allowed for group by
             , P.arg_lists =  igroupkeys : igroupvalues : []
             } =
  do child <- solve ch
     groupvalues <- solveOutputs igroupvalues
     groupkeys <- sequence $ map extractKey igroupkeys
     return Group {child, groupkeys, groupvalues}
       where extractKey P.Expr { P.expr = P.Ref { P.rname }
                               , P.alias = Nothing } = Right rname
             extractKey e_ = Left $ "unexpected expr as group key: " ++ groom e_

 {- Select invariants:
 -single child node
 -predicate is a single scalar expression
 -}


  {- Semijoin invariants:
     - binary relop
     - condition may be complex (most are quality, but some aren't)
  -}


solve s_ = Left $ " parse tree not valid or case not implemented:  " ++ groom s_


fromParseTree :: P.Rel -> Either String RelExpr
fromParseTree = solve

fromString :: String -> Either String RelExpr
fromString s = P.fromString s >>= fromParseTree


{- code to transform parser scalar sublanguage into Mplan scalar -}
sc :: P.ScalarExpr -> Either String ScalarExpr
sc P.Ref { P.rname  } = Right $ Ref rname

{- for now, we are ignoring the aliases within calls -}
sc P.Call { P.fname, P.args = [ P.Expr { P.expr = singlearg, P.alias = _ } ] } =
  do sub <- sc singlearg
     unop <- resolveUnopOpcode fname
     return $ Unary { unop, arg=sub }

sc P.Call { P.fname
          , P.args = [ P.Expr { P.expr = firstarg, P.alias = _ }
                     , P.Expr { P.expr = secondarg, P.alias = _ }
                     ]
          } =
  do left <- sc firstarg
     right <- sc secondarg
     binop <- resolveBinopOpcode fname
     return $ Binop { binop, left, right }

sc P.Cast { P.tspec
          , P.value = P.Expr { P.expr = parg, P.alias = _  }
          } =
  do mtype <- resolveTypeSpec tspec
     arg <- sc parg
     return $ Cast { mtype, arg }

sc P.Literal { P.tspec, P.stringRep } =
  do mtype <- resolveTypeSpec tspec
     int <- ( let r = readIntLiteral stringRep in
              case mtype of
                MInt -> r
                MTinyint -> r
                MSmallint -> r
                _ -> Left "not supporting literals yet"
            )
     return $ IntLiteral int

sc s_ = Left $ "cannot handle this scalar: " ++ groom s_


readIntLiteral :: String -> Either String Int64
readIntLiteral str =
  case reads str :: [(Int64, String)] of
    [(num, [])] -> Right num
    _ -> Left $ "cannot parse as integer literal: " ++ str

