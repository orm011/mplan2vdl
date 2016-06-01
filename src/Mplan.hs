module Mplan( fromParseTree
            , fromString
            , BinaryOp(..)
            , RelExpr(..)
            , ScalarExpr(..)
            , GroupAgg(..)
            , MType(..)) where

import qualified Parser as P
import Name(Name(..))
import Data.Time.Calendar
import Control.DeepSeq(NFData,($!!))
import GHC.Generics (Generic)
import Text.Groom
import Data.Int
import Data.Monoid(mappend)
import Debug.Trace
import Control.Monad(foldM, mapM, void)
import Text.Printf (printf)
import qualified Data.Map.Strict as Map
import Data.List (foldl')
import Data.Time.Format
import Data.Time.Calendar
import Data.Time
import Dict(dictEncode)

type Map = Map.Map

data MType =
  MTinyint
  | MInt
  | MBigInt
  | MSmallint
  | MDate
  | MMillisec
  | MMonth
  | MDouble
  | MChar
  | MDecimal Int Int
  | MSecInterval Int
  | MMonthInterval
  | MBoolean

  deriving (Eq, Show, Generic)
instance NFData MType

isJoinIdx :: P.Attr -> [Name]
isJoinIdx (P.JoinIdx s) = [s]
isJoinIdx _ = []

getJoinIdx :: [P.Attr] -> [Name]
getJoinIdx attrs = foldl' (++) [] (map isJoinIdx attrs)

resolveTypeSpec :: P.TypeSpec -> Either String MType
resolveTypeSpec P.TypeSpec { P.tname, P.tparams } = f tname tparams
  where f "int" [] = Right MInt
        f "tinyint" [] = Right MTinyint
        f "smallint" [] = Right MSmallint
        f "bigint" [] = Right MBigInt
        f "date" []  = Right MDate
        f "char" _ = Right MChar -- ignoring length fields
        f "varchar" [_] = Right MChar --ignore length fields
        f "decimal" [a,b] = Right $ MDecimal a b
        f "sec_interval" [_] = Right MMillisec -- they use millisecs to express their seconds
        f "month_interval" [] = Right MMonth
        f "double" [] = Right MDouble -- used for averages even if columns arent doubles
        f "boolean" [] = Right MBoolean
        f name _ = Left $  "unsupported typespec: " ++ name

resolveCharLiteral :: String -> Either String Int64
resolveCharLiteral ch = dictEncode ch

{- assumes date is formatted properly: todo. error handling for tis -}
resolveDateString :: String -> Int64
resolveDateString datestr =
  fromIntegral $ diffDays day zero
  where zero = (readTime defaultTimeLocale "%Y-%m-%d" "0000-01-01") :: Day
        day = (readTime defaultTimeLocale "%Y-%m-%d" datestr) :: Day

data OrderSpec = Asc | Desc deriving (Eq,Show, Generic)
instance NFData OrderSpec

data BinaryOp =
  Gt | Lt | Leq | Geq {- rel -}
  | Eq | Neq {- comp -}
  | LogAnd | LogOr {- logical -}
  | Sub | Add | Div | Mul | Mod | BitAnd | BitOr | Min | Max  {- arith -}
  deriving (Eq, Show, Generic, Ord)
instance NFData BinaryOp


resolveInfix :: String -> Either String BinaryOp
resolveInfix str =
  case str of
    "<" -> Right Lt
    ">" -> Right Gt
    "<=" -> Right Leq
    ">=" -> Right Geq
    "=" -> Right Eq
    "!=" -> Right Neq
    "or" -> Right LogOr
    _ -> Left $ "unknown infix symbol: " ++ str


resolveBinopOpcode :: Name -> Either String BinaryOp
resolveBinopOpcode nm =
  case nm of
    Name ["sys", "sql_add"] -> Right Add
    Name ["sys", "sql_sub"] -> Right Sub
    Name ["sys", "sql_mul"] -> Right Mul
    Name ["sys", "sql_div"] -> Right Div
    Name ["sys", "sql_min"] -> Right Min
    Name ["sys", "sql_max"] -> Right Max
    _ -> Left $ "unsupported binary function: " ++ show nm

 {- they must be semantically for a single tuple (see aggregates otherwise) -}
data UnaryOp =
  Neg | Year
  deriving (Eq, Show, Generic)
instance NFData UnaryOp


resolveUnopOpcode :: Name -> Either String UnaryOp
resolveUnopOpcode nm =
  case nm of
    Name ["sys", "year"] -> Right Year
    Name ["sys", "sql_neg"] -> Right Neg
    _ -> Left $ "unsupported scalar function " ++ show nm

 {- a Ref can be a column or a previously bound name for an intermediate -}
data ScalarExpr =
  Ref Name
  | IntLiteral Int64 {- use the widest possible type to not lose info -}
  | Unary { unop:: UnaryOp, arg::ScalarExpr }
  | Binop { binop :: BinaryOp, left :: ScalarExpr, right :: ScalarExpr  }
  | Cast { mtype :: MType, arg::ScalarExpr }
  deriving (Eq, Show, Generic)
instance NFData ScalarExpr

data GroupAgg = Sum ScalarExpr | Avg ScalarExpr | Count
  deriving (Eq,Show,Generic)
instance NFData GroupAgg

solveGroupOutputs :: [P.Expr] -> Either String ([(Name, Maybe Name)], [(GroupAgg, Maybe Name)])

solveGroupOutputs exprs =
  do sifted <- sequence $ map ghelper exprs
     return $ foldl' mappend ([],[]) sifted

{- sifts out key lists to the first meber,
 and aggregates to the second  -}
ghelper  :: P.Expr -> Either String ([(Name, Maybe Name)], [(GroupAgg, Maybe Name)])

ghelper P.Expr
  { P.expr = P.Ref {P.rname} {- output key -}
  , P.alias }
  = Right ([(rname, alias)], [])

ghelper P.Expr
  { P.expr = P.Call { P.fname
                    , P.args=[ P.Expr
                               { P.expr=singlearg,
                                 P.alias = _ -- ignoring this inner alias.
                                 }
                             ]
                    }
  , P.alias }
  = do inner <- sc singlearg
       case fname of
         Name ["sys", "sum"] -> return ([], [(Sum inner, alias)])
         Name ["sys", "avg"] -> return ([], [(Avg inner, alias)])
         _ -> Left $ "unknown unary aggregate " ++ show fname

ghelper  P.Expr
  { P.expr = P.Call { P.fname=Name ["sys", "count"]
                    , P.args=[] }
  , P.alias }
  = Right ([], [(Count, alias)] )


ghelper s_ = Left $
  "expression not supported as output of group_by: " ++ (take 50 $ show  s_)


data RelExpr =
  Table       { tablename :: Name
              , tablecolumns :: [(Name, Maybe Name)]
              }
  | Project   { child :: RelExpr
              , projectout :: [(ScalarExpr, Maybe Name)]
              , order ::[(Name, OrderSpec)]
              }
  | Select    { child :: RelExpr
              , predicate :: ScalarExpr
              }
  | GroupBy   { child :: RelExpr
              , inputkeys :: [Name]
              , outputkeys :: [(Name, Maybe Name)]
              , outputaggs :: [(GroupAgg, Maybe Name)]
              }
  | FKJoin    { table :: RelExpr
              , references :: RelExpr
              , idxcol :: Name
              }
  | TopN      { child :: RelExpr
              , n :: Int64
              }
  -- | Cross
  -- | Join
  -- | AntiJoin
  -- | SemiJoin
  -- | LeftOuter
  deriving (Eq,Show, Generic)
instance NFData RelExpr

-- thsis  way to insert extra consistency checks
check :: a -> (a -> Bool) -> String -> Either String ()
check val cond msg = if cond val then Right () else Left msg


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
solve P.Leaf { P.source, P.columns  } =
  do pcols <- sequence $ map split columns
     check pcols ( /= []) "list of table columns must not be empty"
     return $ Table { tablename = source, tablecolumns = pcols}
  where
    split P.Expr { P.expr = P.Ref { P.rname, P.attrs } --attr => no alias
                 , P.alias=Nothing } =
      case getJoinIdx attrs of
        [str] -> Right  (str, Just rname) -- notice reversal
        [] -> Right (rname, Nothing)
        s_ -> Left $ "unexpected: multiple fkey indices" ++ groom attrs
    split P.Expr { P.expr = P.Ref { P.rname, P.attrs } -- alias => no attr
                 , P.alias } =
      case getJoinIdx attrs of
        [] -> Right (rname, alias)
        s_ -> Left $ "unexpected: have both alias and join idx" ++ groom s_
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
     order <- (case rest of
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
     inputkeys <- sequence $ map extractKey igroupkeys
     (outputkeys, outputaggs) <- solveGroupOutputs igroupvalues
     return $ GroupBy {child, inputkeys, outputkeys, outputaggs}
       where extractKey P.Expr { P.expr = P.Ref { P.rname }
                               , P.alias = Nothing } = Right rname
             extractKey e_ =
               Left $ "unexpected alias in input group key: " ++ groom e_

 {- Select invariants:
 -single child node
 -predicate is a single scalar expression
 -}
solve P.Node { P.relop = "select"
             , P.children = [ch]
             , P.arg_lists = props : []
             } =
  do child <- solve ch
     predicate <- conjunction props
     return $ Select { child, predicate }


{-only handling joins where the condition is
done via  a foreign key -}
solve arg@P.Node { P.relop = "join"
             , P.children = [l, r]
             , P.arg_lists =
               [ P.Expr
                 { P.expr=P.Infix
                   { P.infixop = "="
                   , P.left = P.Expr (P.Ref idxcol _) Nothing
                   , P.right = P.Expr (P.Ref _ attrs) Nothing
                   }
                 , P.alias = _}]:[] {-only one condition-}
             } =
  do table  <- solve l
     references <- solve r
     let hasJoinIdx attrs  = filter (\f -> case f of { P.JoinIdx _ -> True; _ -> False; }) attrs  /= []
     check attrs hasJoinIdx $ "need a join index for a fk join at " ++ (take 100 $ show arg)
     return $ FKJoin { table, references, idxcol }

solve arg@P.Node { P.relop="join"
             , P.children= _
             , P.arg_lists=_ } = Left $ "only handling joins via fk right now" ++( take 100 $ show arg)


solve P.Node { P.relop = "top N"
             , P.children = [ch]
             , P.arg_lists = [P.Expr {
                                 -- this kind of "wrd" literal only shows up in the topN oper.
                                 -- so, so far no need to deal with it elsewhere
                                 P.expr = P.Literal
                                 {P.tspec = P.TypeSpec "wrd" [],
                                  P.stringRep },
                              P.alias = Nothing
                                 }
                             ] : []
             } =
  do n  <- readIntLiteral stringRep
     child <- solve ch
     return $ TopN { child , n }

solve s_ = Left $ " case not implemented:  " ++ (take 50 $  show s_)


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

sc arg@P.Literal { P.tspec, P.stringRep } =
  do mtype <- resolveTypeSpec tspec
     ret <- case mtype of
       MDate -> Right $ resolveDateString stringRep
       MChar -> resolveCharLiteral stringRep
       MMillisec -> do r <- readIntLiteral stringRep
                       check r (/= 0) "weird zero interval"
                       let days = quot r (1000 * 60* 60* 24)
                       check days (/= 0)  "check that we are not rounding seconds down to 0 (its possible query is correct, but unlikely in tpch)"
                       return $ days
                       -- millis/secs/mins/hours normalize to days, since thats what tpch uses
       MMonth  -> do months  <- readIntLiteral stringRep -- really, the day value of month intervals isnt that simple,
                          --it depends on the context it is used (eg the actual it is being added to). nvm
                     return $ months * 30
       MDecimal _ _ -> readIntLiteral stringRep -- sql 0.06 shows up as mplan Decimal (,2) "6", so just leave it as is.
       MBoolean  -> (case stringRep of
                        "true" -> Right 1
                        "false" -> Right 0
                        s_ -> Left $ "invalid boolean literal: " ++ show s_)

       _ -> do int <- ( let r = readIntLiteral stringRep in
                        case mtype of
                          MInt -> r
                          MTinyint -> r
                          MSmallint -> r
                          _ -> Left $ "need to handle this literal: " ++ show arg )
               return $ int
     return $ IntLiteral ret


sc P.Infix {P.infixop
           ,P.left
           ,P.right} =
  do l <- sc (P.expr left)
     r <- sc (P.expr right)
     binop <- resolveInfix infixop
     return $ Binop { binop, left=l, right=r }

sc P.Interval {P.ifirst=P.Expr {P.expr=first }
              ,P.firstop
              ,P.imiddle=P.Expr {P.expr=middle }
              ,P.secondop
              ,P.ilast=P.Expr {P.expr=last }
              } =
  do sfirst <- sc first
     smiddle <- sc middle
     slast <- sc last
     fop <- resolveInfix firstop
     sop <- resolveInfix secondop
     let left = Binop { binop=fop, left=sfirst, right=smiddle }
     let right = Binop { binop=sop, left=smiddle, right=slast }
     return $ Binop { binop=LogAnd, left, right}

sc P.In { P.arg = P.Expr { P.expr, P.alias = _}
        , P.negated = False
        , P.set } =
  do exp <- sc expr
     solvedset <- mapM (sc . P.expr) set
     let check x = Binop Eq exp x
     let inOr l r = Binop LogOr (check l) (check r)
     case solvedset of
       [] -> Left "empty 'in' clause"
       x:rest -> return $ foldl' inOr (check x) rest

sc (P.Nested exprs) = conjunction exprs

sc s_ = Left $ "cannot handle this scalar: " ++ (take 50 $ show s_)

{- converts a list into ANDs -}
conjunction :: [P.Expr] -> Either String ScalarExpr
conjunction exprs =
  do solved <- mapM (sc . P.expr) exprs
     case solved of
       [] -> Left $ "conjunction list cannot be empty (should check at parse time)"
       [x] -> return x
       x : y : rest -> return $ foldl' makeAnd (makeAnd x y) rest
       where makeAnd a b = Binop { binop=LogAnd, left=a, right=b }


readIntLiteral :: String -> Either String Int64
readIntLiteral str =
  case reads str :: [(Int64, String)] of
    [(num, [])] -> Right num
    ow -> Left $ printf "cannot parse %s as integer literal: %s" str $ show ow

fromParseTree :: P.Rel -> Either String RelExpr
fromParseTree = solve

fromString :: String -> Either String RelExpr
fromString mplanstring =
  do parsetree <- P.fromString mplanstring
     let mplan = (fromParseTree $!! parsetree) >>= (return . pushFKJoins)
     -- let tr = case mplan of
     --            Left err -> "\n--Error at Mplan stage:\n" ++ err
     --            Right g -> "\n--Mplan output:\n" ++ groom g
     -- trace tr mplan
     mplan


{- this transform pushes select filters
(on the right hand side) above fk joins -}
-- the predicate output names are legal when moved up, bc
-- join does not have bindings in its syntax
-- todo: abstract traversal boilerplate
pushFKJoins :: RelExpr -> RelExpr
pushFKJoins FKJoin { table
                   , references  = Select { child, predicate }
                   , idxcol
                   } =
  Select { child=FKJoin { table = pushFKJoins table, references = pushFKJoins child, idxcol }, predicate }

pushFKJoins b@FKJoin { table, references }
  = b { table=pushFKJoins table, references=pushFKJoins references }

pushFKJoins b@Select { child  }
  = b { child=pushFKJoins child }

pushFKJoins b@GroupBy { child }
  = b { child=pushFKJoins child }

pushFKJoins b@Table { } = b

pushFKJoins b@Project { child }
  = b { child=pushFKJoins child }

pushFKJoins b@TopN { child }
  = b { child=pushFKJoins child }

--undefined for others (TODO: enable error on compiler warning for missing pattern match)
-- for now, do nothing otherwise
-- projects can be pushed as well.
