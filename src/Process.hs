

{- stronger types for each kind of operation -}

data BinaryOp = Gt | Lt | Leq | Geq
  | Eq | Neq {- comp -}
  | Sub | Add | Div | Mul | Mod {- arith -}
  | LogAnd | LogOr
  | BitAnd | BitOr deriving (Eq, Show)


{- custom types for each expression, used after checking parsed contents -}
data RelExpr  = Table { name :: Name, columns :: [(ScalarExpr, Maybe Name)] }
    | Select { rel :: RelExpr, predicate :: ScalarExpr }
    | Project { rel :: RelExpr, values :: [(ScalarExpr, Maybe Name)] }
    | GroupBy { rel :: RelExpr, keys :: [(String, Maybe Name)], values :: [(ScalarExpr, Maybe Name)]  }
    deriving (Eq,Show)


data RelOp = OpSelect
           | OpProject
           | OpGroupBy
           | OpTopK
           | OpCrossProduct
           | OpJoin
           | OpSemiJoin
           | OpAntiJoin
           deriving (Eq,Show)
