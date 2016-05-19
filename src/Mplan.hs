module Mplan( fromParseTree
            , fromString
            , Name
            , BinaryOp
            , RelExpr(..)
            , ScalarExpr(..)) where

import qualified Parser as P
import Parser(Name)


data MType =
  MInt
  | MTinyint
  | MSmallint
  | MDecimal Int Int
  | MSecInterval Int
  | MMonthInterval
  | MDate
  | MCharFix Int
  | MChar
  deriving (Eq, Show)

fromTypeSpec :: P.TypeSpec -> Either String MType
fromTypeSpec P.TypeSpec { P.tname, P.tparams } = f tname tparams
  where f ["int"] [] = Right MInt
        f ["tinyint"] [] = Right MTinyint
        f ["smallint"] [] = Right MSmallint
        f ["decimal"] [a, b] = Right (MDecimal a b)
        f ["sec_interval"] [a] = Right (MSecInterval a)
        f ["month_interval"] []  = Right MMonthInterval
        f ["date"] []  = Right MDate
        f ["char"] [a] = Right (MCharFix a)
        f ["char"] [] = Right MChar
        f _ _ = Left "unsupported typespec"


data OrderSpec = Asc | Desc deriving (Eq,Show)

data BinaryOp =
  Gt | Lt | Leq | Geq {- rel -}
  | Eq | Neq {- comp -}
  | LogAnd | LogOr {- logical -}
  | Sub | Add | Div | Mul | Mod | BitAnd | BitOr  {- arith -}
  deriving (Eq, Show)

data ScalarExpr =
  {- a Ref can be a column or a previously bound name for an intermediate -}
  Ref Name
  | Lit { littype :: MType,  litvalue :: String }
  | Binop { binop :: BinaryOp, left :: ScalarExpr, right :: ScalarExpr  }
  deriving (Eq, Show)

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
              , groupvalues :: [(Name, Maybe Name)]
              , groupkeys :: [(Name, Maybe Name)]
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
  deriving (Eq,Show)


-- thsis  way to insert extra consistency checks
check :: a -> (a -> Bool) -> String -> Either String a
check val cond msg = if cond val then Right val else Left msg

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
  where solveOutputs explist = sequence $ map f explist
        f P.Expr { P.expr, P.alias } =
          do scalar <- sc expr
             return (scalar, alias)


  {- Select invariants:
     -single child node
     -predicate is a single scalar expression
  -}

  {- Group invariants:
     - single child node
     - multiple output value columns with potential expressions (non-empty)
     - multiple group key value columns (non-empty)
  -}
  {- Semijoin invariants:
     - binary relop
     - condition may be complex (most are quality, but some aren't)
  -}


solve _ = Left $ " parse tree not valid or case not implemented  "


fromParseTree :: P.Rel -> Either String RelExpr
fromParseTree = solve

fromString :: String -> Either String RelExpr
fromString s = P.fromString s >>= fromParseTree


{- code to transform parser scalar sublanguage into Mplan scalar -}
sc :: P.ScalarExpr -> Either String ScalarExpr
sc P.Ref { P.rname  } = Right $ Ref rname

sc P.Literal { P.tspec, P.stringRep } =
  do tp <- fromTypeSpec tspec
     -- finish later.
     Left "not supporting literals yet"
     -- Lit { littype=tspec,  litvalue=stringRep }


sc _ = Left "problem with p.scalar"
