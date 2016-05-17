module MPlan(fromParseTree, fromString) where


import qualified Parser as P

type Name = P.Name {- use the same qualified name type -}

data OrderSpec = Asc | Desc deriving (Eq,Show)

data BinaryOp =
  Gt | Lt | Leq | Geq {- rel -}
  | Eq | Neq {- comp -}
  | LogAnd | LogOr {- logical -}
  | Sub | Add | Div | Mul | Mod | BitAnd | BitOr  {- arith -}
  deriving (Eq, Show)

data ScalarExpr =
  {- a Ref can be a column or a previously bound name for an intermediate -}
  Ref  { refname :: Name }
  | Lit { littype :: String,  litvalue :: String }
  | Binop { binop :: BinaryOp, left :: ScalarExpr, right :: ScalarExpr  }
  deriving (Eq, Show)

data RelExpr =
  {- Table invariants:
     -tablecolumns must not be empty.
  -}
  Table { tablename :: Name,  tablecolumns :: [(Name, Maybe Name)]  }
  {- Select invariants:
     -single child node
     -predicate is a single scalar expression
  -}
  | Select { child :: RelExpr, selectpredicate :: ScalarExpr  }
  {- Project invariants
      - single child node
      - multiple output columns with potential aliasing (non empty)
      - potentially empty order columns. no aliasing there. (what relation do they have with output ones?)
  -}
  | Project { child :: RelExpr, projectout :: [(Name, Maybe Name)], order ::[(Name, OrderSpec)] }
  {- Group invariants:
     - single child node
     - multiple output value columns with potential expressions (non-empty)
     - multiple group key value columns (non-empty)
  -}
  | Group {  child :: RelExpr, groupvalues :: [(Name, Maybe Name)], groupkeys :: [(Name, Maybe Name)]  }
  {- Semijoin invariants:
     - binary relop
     - condition may be complex (most are quality, but some aren't)
  -}
  | SemiJoin { lchild :: RelExpr, rchild :: RelExpr, condition :: ScalarExpr  }
  | TopN
  | Cross
  | Join
  | AntiJoin
  | LeftOuter
  deriving (Eq,Show)


fromParseTree :: P.Rel -> Either String RelExpr
fromParseTree _ = Left " not implemented "

fromString :: String -> Either String RelExpr
fromString s = P.fromString s >>= fromParseTree
