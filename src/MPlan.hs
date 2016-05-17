module MPlan with
import Parser (fromString)

{- stronger types for each kind of operation -}
data BinaryOp =
  Gt | Lt | Leq | Geq {- rel -}
  | Eq | Neq {- comp -}
  | Sub | Add | Div | Mul | Mod {- arith -}
  | LogAnd | LogOr {- only used in where clause, it seems -}
  | BitAnd | BitOr deriving (Eq, Show)

{- custom variants for each relational expression, with tighter constraints -}
data RelExpr  = Table { name :: Name, columns :: [(ScalarExpr, Maybe Name)] }
    | Select { rel :: RelExpr, predicate :: ScalarExpr }
    | Project { rel :: RelExpr, values :: [(ScalarExpr, Maybe Name)] }
    | GroupBy { rel :: RelExpr, keys :: [(String, Maybe Name)], values :: [(ScalarExpr, Maybe Name)]  }
    deriving (Eq,Show)

data RelOp = OpSelect
           | OpProject
           | OpGroup
           | OpTopN
           | OpCross
           | OpJoin
           | OpSemiJoin
           | OpAntiJoin
           deriving (Eq,Show)

