module Mplan( mplanFromParseTree
            , pushFKJoins
            , fuseSelects
            , BinaryOp(..)
            , UnaryOp(..)
            , RelExpr(..)
            , ScalarExpr(..)
            , GroupAgg(..)
            , FoldOp(..)
            , MType(..)) where

import qualified Parser as P
import Name(Name(..),TypeSpec(..))
import Data.Time.Calendar
import Control.DeepSeq(NFData)
import GHC.Generics (Generic)
import Data.Hashable
--import Data.Int
--import Data.Monoid(mappend)
--import Debug.Trace
--import Control.Monad(foldM, mapM, void)
--import qualified Data.Map.Strict as Map
import Data.Time.Format
--import Data.Time.Calendar
--import Data.Time
import Dict(dictEncode)
import Data.Data
import Data.Generics.Uniplate.Data
import Config
import qualified Error as E
import Error(check)
import Data.List(foldl')
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C
--import qualified Data.List.NonEmpty as N
import Data.List.NonEmpty(NonEmpty(..))

--type Map = Map.Map

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
  | MDecimal Integer Integer
  | MSecInterval Integer
  | MMonthInterval
  | MBoolean
  deriving (Eq, Show, Generic, Data)
instance NFData MType

isJoinIdx :: P.Attr -> [Name]
isJoinIdx (P.JoinIdx s) = [s]
isJoinIdx _ = []

getJoinIdx :: [P.Attr] -> [Name]
getJoinIdx attrs = foldl' (++) [] (map isJoinIdx attrs)

resolveTypeSpec :: TypeSpec -> MType
resolveTypeSpec TypeSpec { tname, tparams } = f tname tparams
  where f "int" [] = MInt
        f "tinyint" [] = MTinyint
        f "smallint" [] = MSmallint
        f "bigint" [] = MBigInt
        f "date" []  = MDate
        f "char" _ =  MChar -- ignoring length fields
        f "varchar" [_] =  MChar --ignore length fields
        f "decimal" [a,b] = MDecimal a b
        f "sec_interval" [_] = MMillisec -- they use millisecs to express their seconds
        f "month_interval" [] = MMonth
        f "double" [] = MDouble -- used for averages even if columns arent doubles
        f "boolean" [] = MBoolean
        f name _ = error $ "unsupported typespec: " ++ show name

resolveCharLiteral :: B.ByteString -> Either String Integer
resolveCharLiteral ch = dictEncode (C.unpack ch)

{- assumes date is formatted properly: todo. error handling for tis -}
resolveDateString :: B.ByteString -> Integer
resolveDateString datestr =
  diffDays day zero
  where zero = (parseTimeOrError True defaultTimeLocale "%Y-%m-%d" "0000-01-01") :: Day
        day = (parseTimeOrError True defaultTimeLocale "%Y-%m-%d" (C.unpack datestr)) :: Day

data OrderSpec = Asc | Desc deriving (Eq,Show, Generic, Data)
instance NFData OrderSpec

data BinaryOp =
  Gt | Lt | Leq | Geq {- rel -}
  | Eq | Neq {- comp -}
  | LogAnd | LogOr {- logical -}
  | Sub | Add | Div | Mul | Mod | BitAnd | BitOr | Min | Max | BitShift  {- arith -}
  deriving (Eq, Show, Generic,Data)
instance NFData BinaryOp
instance Hashable BinaryOp

resolveInfix :: B.ByteString -> Either String BinaryOp
resolveInfix str =
  case str of
    "<" -> Right Lt
    ">" -> Right Gt
    "<=" -> Right Leq
    ">=" -> Right Geq
    "=" -> Right Eq
    "!=" -> Right Neq
    "or" -> Right LogOr
    _ -> Left $ E.unexpected "infix symbol" str


resolveBinopOpcode :: Name -> Either String BinaryOp
resolveBinopOpcode nm =
  case nm of
    Name ["sql_add"] -> Right Add
    Name ["sql_sub"] -> Right Sub
    Name ["sql_mul"] -> Right Mul
    Name ["sql_div"] -> Right Div
    Name ["sql_min"] -> Right Min
    Name ["sql_max"] -> Right Max
    Name ["="] -> Right Eq
    Name ["or"] -> Right LogOr
    Name ["and"] -> Right LogAnd
    Name [">"] -> Right Gt
    Name ["<>"] -> Right Neq
    _ -> Left $ E.unexpected "binary function" nm


  {- they must be semantically for a single tuple (see aggregates otherwise) -}
data UnaryOp =
  Neg | Year | IsNull
  deriving (Eq, Show, Generic, Data)
instance NFData UnaryOp

resolveUnopOpcode :: Name -> Either String UnaryOp
resolveUnopOpcode nm =
  case nm of
    Name ["year"] -> Right Year
    Name ["sql_neg"] -> Right Neg
    Name ["isnull"] -> Right IsNull
    _ -> Left $ E.unexpected "scalar function " nm

 {- a Ref can be a column or a previously bound name for an intermediate -}
data ScalarExpr =
  Ref Name
  | IntLiteral Integer {- use the widest possible type to not lose info -}
  | Unary { unop:: UnaryOp, arg::ScalarExpr }
  | Binop { binop :: BinaryOp, left :: ScalarExpr, right :: ScalarExpr  }
  | IfThenElse { if_::ScalarExpr, then_::ScalarExpr, else_::ScalarExpr }
  | Cast { mtype :: MType, arg::ScalarExpr }
  | In { left :: ScalarExpr, set :: [ScalarExpr]}
  | Like {ldata::ScalarExpr, lpattern::C.ByteString}
  deriving (Eq, Show, Generic, Data)
instance NFData ScalarExpr

data FoldOp = FSum | FMax deriving (Eq,Show,Generic,Data)
instance NFData FoldOp

data GroupAgg = GDominated Name |  GAvg ScalarExpr | GCount | GFold FoldOp ScalarExpr deriving (Eq,Show,Generic,Data)
instance NFData GroupAgg

solveGroupOutput :: P.Expr -> Either String (GroupAgg, Maybe Name)

solveGroupOutput P.Expr
  { P.expr = P.Ref {P.rname} {- output key -}
  , P.alias }
  = let outname = case alias of
          Nothing -> Just rname
          Just _ -> alias
    in Right $ (GDominated rname, outname)

solveGroupOutput P.Expr
  { P.expr = P.Call { P.fname=Name ["count"]
                    , P.args=[] }
  , P.alias }
  = Right $ (GCount, alias)

solveGroupOutput P.Expr
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
         Name ["sum"] -> Right $ (GFold FSum inner, alias)
         Name ["avg"] -> Right $ (GAvg inner, alias)
         Name ["max"] -> Right $ (GFold FMax inner, alias)
         _ -> Left $ E.unexpected  "unary aggregate" fname

solveGroupOutput  s_ = Left $ E.unexpected "group_by output expression" s_


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
              , outputaggs :: [(GroupAgg, Maybe Name)]
              }
  | Join    { leftch :: RelExpr -- we will figure out if there is an fk  join later.
            , rightch :: RelExpr
            , conds :: NonEmpty ScalarExpr
            }
  | TopN      { child :: RelExpr
              , n :: Integer
              }
  deriving (Eq,Show, Generic, Data)
instance NFData RelExpr


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
    split P.Expr { P.expr = P.Ref { P.rname, P.attrs } --no alias. then use attr or name
                 , P.alias=Nothing } =
      case getJoinIdx attrs of
        [fkcol] -> Right  (fkcol, Just rname) -- notice reversal
        [] -> Right (rname, Nothing)
        s_ -> Left $ E.unexpected " multiple fkey indices" s_
    split P.Expr { P.expr = P.Ref { P.rname, P.attrs }
                 , P.alias=as@(Just _) } =
      case getJoinIdx attrs of
        [] -> Right (rname, as) --
        [fkcol] -> Right (fkcol, as)  -- here, we have both an alias and joinidx (eg q8 nation_fk1)
        _ -> Left $ E.unexpected " multiple fkey indices" attrs
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
                 _ -> Left $ E.unexpected "order-by clauses" rest)
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
     outputaggs  <- mapM solveGroupOutput igroupvalues
     return $ GroupBy {child, inputkeys, outputaggs}
       where extractKey P.Expr { P.expr = P.Ref { P.rname }
                               , P.alias = Nothing } = Right rname
             extractKey e_ =
               Left $ E.unexpected "alias in input group key" e_

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
solve P.Node { P.relop
             , P.children = [l, r]
             , P.arg_lists =
               conditions:[]
               {- remember : its a list of lists -}
             }
  | ("semijoin" == relop || "join" == relop) =
    do leftch  <- solve l
       rightch <- solve r
       prelim_conds <- mapM ( sc . P.expr) conditions
       let conds = case prelim_conds of
             [] -> error " empty join condition list is invalid "
             first : rest -> first :| rest
       return $ Join { leftch, rightch, conds }

solve P.Node { P.relop = "top N"
             , P.children = [ch]
             , P.arg_lists = [P.Expr {
                                 -- this kind of "wrd" literal only shows up in the topN oper.
                                 -- so, so far no need to deal with it elsewhere
                                 P.expr = P.Literal
                                 {P.tspec = TypeSpec "wrd" [],
                                  P.stringRep },
                              P.alias = Nothing
                                 }
                             ] : []
             } =
  do n  <- readIntLiteral stringRep
     child <- solve ch
     return $ TopN { child , n }

solve P.Node { P.relop } = Left $ E.unexpected "relational operator not implemented" relop

{- code to transform parser scalar sublanguage into Mplan scalar -}
sc :: P.ScalarExpr -> Either String ScalarExpr
sc P.Ref { P.rname  } = Right $ Ref rname


-- TODO: doublecheck this is correct
-- (see query 17 for an example plan using it)
sc P.Call { P.fname=Name ["identity"]
          , P.args = [ P.Expr { P.expr } ]
          } = -- rewrite into just inner expression
  sc expr

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


sc P.Call { P.fname=Name ["ifthenelse"]
          , P.args = [ P.Expr { P.expr = mif, P.alias = _ }
                     , P.Expr { P.expr = mthen, P.alias = _ }
                     , P.Expr { P.expr = melse, P.alias = _ }
                     ]
          } =
  do if_ <- sc mif
     then_ <- sc mthen
     else_ <- sc melse
     return $ IfThenElse { if_, then_, else_ }

sc P.Cast { P.tspec
          , P.value = P.Expr { P.expr = parg, P.alias = _  }
          } =
  do let mtype = resolveTypeSpec tspec
     arg <- sc parg
     return $ Cast { mtype, arg }

sc arg@P.Literal { P.tspec, P.stringRep } =
  do let mtype = resolveTypeSpec tspec
     ret <- case mtype of
       MDate -> Right $ resolveDateString stringRep
       MChar -> resolveCharLiteral stringRep
       MMillisec ->
         do r <- readIntLiteral stringRep
            check r (/= 0) "weird zero interval"
            let days = quot r (1000 * 60* 60* 24)
            check days (/= 0)  "check that we are not rounding seconds down to 0 (its possible query is correct, but unlikely in tpch)"
            return $ days
            -- millis/secs/mins/hours normalize to days,
            --since thats what tpch uses
       MMonth  ->
         do months  <- readIntLiteral stringRep
            return $ months * 30
            -- really, the day value of month intervals isnt that simple,
            --it depends on the context it is used
            --(eg the actual it is being added to). nvm
       MDecimal _ _ -> readIntLiteral stringRep
       -- sql 0.06 shows up
       -- as mplan Decimal (,2) "6", so just leave it as is.
       MBoolean  -> (case stringRep of
                        "true" -> Right 1
                        "false" -> Right 0
                        s_ -> Left $ E.unexpected "boolean literal" s_)

       _ -> do int <- ( let r = readIntLiteral stringRep in
                        case mtype of
                          MInt -> r
                          MTinyint -> r
                          MSmallint -> r
                          _ -> Left $ E.unexpected "literal" arg )
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
              ,P.ilast=P.Expr {P.expr=lastel }
              } =
  do sfirst <- sc first
     smiddle <- sc middle
     slast <- sc lastel
     fop <- resolveInfix firstop
     sop <- resolveInfix secondop
     let left = Binop { binop=fop, left=sfirst, right=smiddle }
     let right = Binop { binop=sop, left=smiddle, right=slast }
     return $ Binop { binop=LogAnd, left, right}


sc P.In { P.arg = P.Expr { P.expr, P.alias = _}
        , P.negated = False
        , P.set } =
  do sleft <- sc expr
     sset <- mapM (sc . P.expr) set
     return $ In { left=sleft, set=sset }

sc (P.Nested exprs) = conjunction exprs


sc P.Filter { P.oper="like"
            , P.arg=P.Expr {P.expr=arg}
            , P.negated -- aka ! or nothing
            , P.pattern=P.Expr { P.expr=P.Cast {
                                   P.tspec = TypeSpec {tname="char", tparams=[]}
                                   , P.value= P.Expr { P.expr=P.Literal {
                                                         P.tspec=TypeSpec {tname="char", tparams=[_]}
                                                         , P.stringRep=lpattern
                                                         }
                                                     } -- aka char [char($_) "$1"]
                                   }
                               , P.alias=Nothing }
            , P.escape=P.Literal { P.tspec=TypeSpec { tname="char", tparams=[] }
                                 , P.stringRep = "" } --aka char ""
            }  =
  do sarg <- sc arg
     let like = Like { ldata=sarg, lpattern }
     return $ if negated then Unary { unop=Neg, arg=like } else like

sc P.Filter { P.oper } =
   Left $ E.unexpected "operator" oper

sc s_ = Left $ E.unexpected "scalar operator" s_

{- converts a list into ANDs -}
conjunction :: [P.Expr] -> Either String ScalarExpr
conjunction exprs =
  do solved <- mapM (sc . P.expr) exprs
     case solved of
       [] -> Left $ E.unexpected  "empty conjunction list" exprs
       [x] -> return x
       x : y : rest -> return $ foldl' makeAnd (makeAnd x y) rest
       where makeAnd a b = Binop { binop=LogAnd, left=a, right=b }


readIntLiteral :: B.ByteString -> Either String Integer
readIntLiteral str =
  case (C.readInteger str :: Maybe (Integer, B.ByteString)) of
    Just (num, "") -> Right num
    _ -> Left $ E.unexpected "integer literal" str

mplanFromParseTree :: P.Rel -> Config -> Either String RelExpr
mplanFromParseTree _1 _ = solve _1

-- The predicate output names are legal when moved up, bc
-- join does not have bindings in its syntax.
-- Note: this code will repeat the transform below
-- until it no longer applies to the data structure (see uniplate package)
pushFKJoins :: RelExpr -> RelExpr
pushFKJoins = rewrite swap
  where -- pattern order matters in terms of which gets preferred
    --dimension table selects get pushed up as well, after fact table ones
    swap Join { leftch
                , rightch = Select { child
                                   , predicate }
                , conds=cond :| []
                } =
      Just $ Select { child=Join { leftch
                                     , rightch = child
                                     , conds=cond :| [] }
                    , predicate }
    -- fact table selects get pushed up as well, after dim table
    -- NOTE: the predicate merge step is meant to map bottommost to leftmost
    swap Join { leftch = Select { child=selectchild -- push left select
                                   , predicate }
                  , rightch
                  , conds=cond :| []
                  } =
      Just $ Select { child=Join { leftch = selectchild
                                     , rightch
                                     , conds=cond :| [] }
                    , predicate }
    swap _ = Nothing


fuseSelects :: RelExpr -> RelExpr
fuseSelects = rewrite fuse
  where
    fuse Select { child=Select { child=grandchild
                               , predicate=bottompred }
                , predicate=toppred
                } =
      -- bottompred is left (since we still want it to apply before top pred)
      Just $ Select { child=grandchild
                    , predicate= Binop {binop=LogAnd
                                       , left=bottompred
                                       , right=toppred}
                    }
    fuse _ = Nothing
