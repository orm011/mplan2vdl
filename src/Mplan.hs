module Mplan( mplanFromParseTree
            , pushFKJoins
            , fuseSelects
            , BinaryOp(..)
            , UnaryOp(..)
            , RelExpr(..)
            , ScalarExpr(..)
            , GroupAgg(..)
            , FoldOp(..)
            , JoinVariant(..)
            , MType(..)) where

import qualified Parser as P
import Name(Name(..),TypeSpec(..))
import Data.Time.Calendar
import Control.DeepSeq(NFData)
import GHC.Generics (Generic)
import Data.Hashable
import Control.Exception.Base
--import Data.Int
--import Data.Monoid(mappend)
import Debug.Trace
--import Control.Monad(foldM, mapM, void)
--import qualified Data.Map.Strict as Map
import Data.Time.Format
--import Data.Time.Calendar
--import Data.Time
--import Dict(dictEncode)
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
--import qualified Data.Map.Strict as Map
--type Map = Map.Map
import qualified Data.HashMap.Strict as HashMap

isJoinIdx :: P.Attr -> [Name]
isJoinIdx (P.JoinIdx s) = [s]
isJoinIdx _ = []

getJoinIdx :: [P.Attr] -> [Name]
getJoinIdx attrs = foldl' (++) [] (map isJoinIdx attrs)

resolveCharLiteral  :: Config -> B.ByteString -> Integer
resolveCharLiteral config ch = case HashMap.lookup ch (dictionary config) of
  Nothing -> error $  "not found in dictionary: " ++ (C.unpack ch)
  Just x -> trace (",," ++ C.unpack ch ++"," ++ show x)  $ x

parseDate :: C.ByteString -> Day
parseDate datestr =
  parseTimeOrError True defaultTimeLocale "%Y-%m-%d" (C.unpack datestr)

{- assumes date is formatted properly: todo. error handling for tis -}
dayCount :: Day -> Integer
dayCount day =
  let zero = parseDate "0000-01-01"
  in diffDays day zero

resolveDateString :: B.ByteString -> Integer
resolveDateString datestr = dayCount $ parseDate datestr

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
    Name ["scale_down"] -> Right Div
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
  | Literal SType Integer -- the integer encodes the representation of the value.
  | Unary { unop:: UnaryOp, arg::ScalarExpr }
  | Binop { binop :: BinaryOp, left :: ScalarExpr, right :: ScalarExpr  }
  | IfThenElse { if_::ScalarExpr, then_::ScalarExpr, else_::ScalarExpr }
  | Cast { mtype :: MType, arg::ScalarExpr }
  | In { left :: ScalarExpr, set :: [ScalarExpr]}
  | Like {ldata::ScalarExpr, lpattern::C.ByteString}
  deriving (Eq, Show, Generic, Data)
instance NFData ScalarExpr

-- fchoose for dominating.
data FoldOp = FSum | FMax | FMin | FChoose  deriving (Eq,Show,Generic,Data)
instance NFData FoldOp

-- GCount is count(*) rows on a table
data GroupAgg = GAvg ScalarExpr | GCount | GFold FoldOp ScalarExpr deriving (Eq,Show,Generic,Data)
instance NFData GroupAgg

solveGroupOutput :: Config -> P.Expr -> Either String (GroupAgg, Maybe Name)

solveGroupOutput _ P.Expr
  { P.expr = P.Ref {P.rname} {- output key -}
  , P.alias }
  = let outname = case alias of
          Nothing -> Just rname
          Just _ -> alias
    in Right $ (GFold FChoose (Ref rname), outname)


-- this is count(*). counts everything. equal to a sum of 1s.
solveGroupOutput _ P.Expr
  { P.expr = P.Call { P.fname=Name ["count"]
                    , P.args=[] }
  , P.alias }
  = Right $ (GCount, alias)

solveGroupOutput config P.Expr
  { P.expr = P.Call{  P.fname
                    , P.args=[ P.Expr
                               { P.expr=singlearg,
                                 P.alias = _ -- ignoring this inner alias.
                                 }
                             ]
                    }
  , P.alias }
  = do inner <- sc config singlearg
       case fname of
         Name ["sum"] -> Right $ (GFold FSum inner, alias)
         Name ["avg"] -> Right $ (GAvg inner, alias)
         Name ["max"] -> Right $ (GFold FMax inner, alias)
         Name ["min"] -> Right $ (GFold FMin inner, alias)
         Name ["count"]
           | P.Ref _ _ <- singlearg -> Right $ (GCount, alias) -- this is count(col). counts only non null...
             -- For now, deal with counting this way, which is incorrect.
-- In truth, We would need to have a separate vector to know which entries of this vector are null.
-- the only query using this (q 16?) seems to have it be not-null anyway.
             -- monet does not seem to give an answer if the inner expression is not a column name.
         _ -> Left $ E.unexpected  "unary aggregate" fname

solveGroupOutput  _ s_ = Left $ E.unexpected "group_by output expression" s_

data JoinVariant = Plain | LeftSemi | LeftOuter | LeftAnti deriving (Show,Eq, Generic,Data)
instance NFData JoinVariant

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
              , inputkeys :: [(Name, Maybe Name)]
                             -- alias used in query 16.
              , outputaggs :: [(GroupAgg, Maybe Name)]
              }
  | Join    { leftch :: RelExpr -- we will figure out if there is an fk  join later.
            , rightch :: RelExpr
            , conds :: NonEmpty ScalarExpr
            , joinvariant :: JoinVariant
            }
  | TopN      { child :: RelExpr
              , n :: Integer
              }
  deriving (Eq,Show, Generic, Data)
instance NFData RelExpr


{-helper function uesd in multiple operators that introduce output
columns -}
solveOutputs :: Config -> [P.Expr] -> Either String [(ScalarExpr, Maybe Name)]
solveOutputs config explist = sequence $ map f explist
  where f P.Expr { P.expr, P.alias } =
          do scalar <- sc config expr
             return (scalar, alias)


solve :: Config -> P.Rel -> Either String RelExpr

{- Leaf (aka Table) invariants /checks:
  -tablecolumns must not be empty.
  -table columns must only be references, not complex expressions nor literals.
  -table columns may themselves be aliased within table.
  -some of the names involve using schema (not for now)
   for concrete resolution to things like partsupp.%partsupp_fk1
-}
solve _ P.Leaf { P.source, P.columns  } =
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

solve config P.Node { P.relop = "project"
                    , P.children = [ch] -- only one child rel allowed for project
                    , P.arg_lists = out : rest -- not dealing with order by right now.
                    } =
  do child <- solve config ch
     projectout <- solveOutputs config out
     order <- (case rest of
                 [] -> Right []
                 _ -> Left $ E.unexpected "order-by clauses" rest)
     return $ Project {child, projectout, order }


  {- Group invariants:
     - single child node
     - multiple output value columns with potential expressions (non-empty)
     - multiple group key value columns (non-empty)
  -}
solve config P.Node { P.relop = "group by"
                    , P.children = [ch] -- only one child rel allowed for group by
                    , P.arg_lists =  igroupkeys : igroupvalues : []
                    } =
  do child <- solve config ch
     let inputkeys = map extractKey igroupkeys
     outputaggs  <- mapM (solveGroupOutput config) igroupvalues
     return $ GroupBy {child, inputkeys, outputaggs}
       where extractKey P.Expr { P.expr = P.Ref { P.rname }
                               , P.alias } = (rname, alias)
             extractKey _ = error "non-ref in group by key"

 {- Select invariants:
 -single child node
 -predicate is a single scalar expression
 -}
solve config P.Node { P.relop = "select"
                    , P.children = [ch]
                    , P.arg_lists = props : []
                    } =
  do child <- solve config ch
     predicate <- conjunction config props
     return $ Select { child, predicate }


solve config P.Node { P.relop
                    , P.children = [l, r]
                    , P.arg_lists =
                      conditions:[]
                 {- remember : its a list of lists -}
                    }
  | ("semijoin" == relop || "join" == relop || "antijoin" == relop || "left outer join" == relop) =
    do leftch  <- solve config l
       rightch <- solve config r
       let joinvariant = case relop of
             "join" -> Plain
             "semijoin" -> LeftSemi
             "antijoin" -> LeftAnti
             "left outer join" -> LeftOuter
             _ -> error $ "no other joins expected: "  ++ C.unpack relop
       prelim_conds <- mapM ( (sc config) . P.expr) conditions
       let conds = case prelim_conds of
             [] -> error " empty join condition list is invalid "
             first : rest -> first :| rest
       return $ Join { leftch, rightch, conds, joinvariant }

solve config  P.Node { P.relop = "top N"
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
  do let n = readIntLiteral stringRep
     child <- solve config ch
     return $ TopN { child , n }

solve _ P.Node { P.relop } = Left $ E.unexpected "relational operator not implemented" relop

{- code to transform parser scalar sublanguage into Mplan scalar -}
sc :: Config -> P.ScalarExpr -> Either String ScalarExpr
sc _ P.Ref { P.rname  } = Right $ Ref rname


-- reweites things like
-- sys.sql_add(date "1996-01-01", month_interval "3")
-- sys.sql_sub(date "1998-12-01", sec_interval(4) "7776000000") that show up in tpch mplans.
-- into a date literal that then gets handled like other plain date literals
sc config P.Call { P.fname = Name [op]
                 , P.args = [ P.Expr { P.expr=P.Literal {P.tspec=TypeSpec {tname="date"}, P.stringRep=datestr} },
                              P.Expr { P.expr=P.Literal {P.tspec=TypeSpec {tname}, P.stringRep=intervalstr} }
                            ]
                 }
  | (tname `elem` ["month_interval","sec_interval"]) && (op `elem` ["sql_add", "sql_sub"]) =
     let date = parseDate datestr
         rawnum = readIntLiteral intervalstr
         num = case op of
           "sql_sub" -> -rawnum
           "sql_add" -> rawnum
           _ -> error "unknown date interval op"
         outdate = case tname of
           "month_interval" -> addGregorianMonthsRollOver num date
           "sec_interval" ->  let kMillisInDay = (1000*60*60*24) --days, hours, minutes and seconds all reprd as millisecs
                                  days = num `quot` kMillisInDay
                              in addDays days date
           _ -> error "unknown interval type"
         stringRep = C.pack $ show outdate
         newexpr = traceShowId $ P.Literal {P.tspec=TypeSpec {tname="date", tparams=[]}, P.stringRep}
     in sc config newexpr

-- TODO: doublecheck this is correct
-- (see query 17 for an example plan using it)
sc config P.Call { P.fname=Name ["identity"]
          , P.args = [ P.Expr { P.expr } ]
          } = -- rewrite into just inner expression
  sc config expr


-- one of two versions of like in the plans:
sc config P.Call { P.fname=Name ["like"]
          , P.args = [ P.Expr { P.expr=likearg }
                     , P.Expr {
                         P.expr=P.Cast {
                             P.value=P.Expr {
                                 P.expr=P.Literal {
                                     P.tspec=TypeSpec { tname="char"
                                                      , tparams=[_]
                                                      }
                                     , P.stringRep=likepattern
                                     }
                                 }
                             , P.tspec=TypeSpec { tname="char", tparams=[] }
                             }
                         }
                     ]
          } =
  do ldata <- sc config likearg
     return $ Like { ldata, lpattern=likepattern }

sc _ P.Call { P.fname=Name ["like"] } = error "implement this 'like' case"

{- for now, we are ignoring the aliases within calls -}
sc config P.Call { P.fname, P.args = [ P.Expr { P.expr = singlearg, P.alias = _ } ] } =
  do sub <- sc config singlearg
     unop <- resolveUnopOpcode fname
     return $ Unary { unop, arg=sub }

sc config P.Call { P.fname
          , P.args = [ P.Expr { P.expr = firstarg, P.alias = _ }
                     , P.Expr { P.expr = secondarg, P.alias = _ }
                     ]
          } =
  do left <- sc config firstarg
     right <- sc config secondarg
     binop <- resolveBinopOpcode fname
     return $ Binop { binop, left, right }


sc config P.Call { P.fname=Name ["ifthenelse"]
          , P.args = [ P.Expr { P.expr = mif, P.alias = _ }
                     , P.Expr { P.expr = mthen, P.alias = _ }
                     , P.Expr { P.expr = melse, P.alias = _ }
                     ]
          } =
  do if_ <- sc config mif
     then_ <- sc config mthen
     else_ <- sc config melse
     return $ IfThenElse { if_, then_, else_ }

sc config P.Cast { P.tspec
          , P.value = P.Expr { P.expr = parg, P.alias = _  }
          } =
  do let mtype = resolveTypeSpec tspec
     arg <- sc config parg
     return $ Cast { mtype, arg }

sc config arg@P.Literal { P.tspec, P.stringRep } =
  do let mtype = resolveTypeSpec tspec
     let (stype,repr) = case mtype of
           -- eg q1:
           -- lineitem.l_shipdate NOT NULL <= sys.sql_sub(date "1998-12-01", sec_interval(4) "7776000000").
           -- if shipdate is stored as an integer representing days.
           -- then, if
           MDate -> (SInt32, resolveDateString stringRep)
           MMillisec -> let r = readIntLiteral stringRep
                            millis_in_a_day = (1000 * 60* 60* 24)
                            days = r `quot` millis_in_a_day
                            leftover = r `rem` millis_in_a_day
                        in assert (r > 0) (assert (leftover == 0)  (SInt32, days))
           MMonth  -> let months = readIntLiteral stringRep
                      in (SInt32, months * 30)
           MDecimal a b -> (SDecimal{precision=a, scale=b}, readIntLiteral stringRep)
           -- sql 0.06 shows up
           -- as mplan Decimal (,2) "6", so just reinterpret int as decimal.
           MBoolean  -> (case stringRep of
                            "true" -> (SInt32, 1)
                            "false" -> (SInt32, 0)
                            _ -> error $ "unknown invalid boolean literal: " ++ show arg)
           MTinyint -> (SInt32, readIntLiteral stringRep)
           MSmallint -> (SInt32, readIntLiteral stringRep)
           MInt -> (SInt32, readIntLiteral stringRep)
           MBigInt -> (SInt64, readIntLiteral stringRep)
           -- the problem here is that we don't really know the bitwidth...
           -- so assume the worst (likely it won't match columns it is used against)
           MChar _ -> (SInt64, resolveCharLiteral config stringRep) -- assume 8 bytes is enough...
           _ -> error $ "need to support literal of this type: " ++ show arg
     return $ Literal stype repr

sc config P.Infix {P.infixop
           ,P.left
           ,P.right} =
  do l <- sc config (P.expr left)
     r <- sc config (P.expr right)
     binop <- resolveInfix infixop
     return $ Binop { binop, left=l, right=r }

sc config P.Interval {P.ifirst=P.Expr {P.expr=first }
              ,P.firstop
              ,P.imiddle=P.Expr {P.expr=middle }
              ,P.secondop
              ,P.ilast=P.Expr {P.expr=lastel }
              } =
  do sfirst <- sc config first
     smiddle <- sc config middle
     slast <- sc config lastel
     fop <- resolveInfix firstop
     sop <- resolveInfix secondop
     let left = Binop { binop=fop, left=sfirst, right=smiddle }
     let right = Binop { binop=sop, left=smiddle, right=slast }
     return $ Binop { binop=LogAnd, left, right}


sc config P.In { P.arg = P.Expr { P.expr, P.alias = _}
        , P.negated = False
        , P.set } =
  do sleft <- sc config expr
     sset <- mapM ((sc config) . P.expr) set
     return $ In { left=sleft, set=sset }

sc config (P.Nested exprs) = conjunction config exprs


sc config P.Filter { P.oper="like"
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
  do sarg <- sc config arg
     let like = Like { ldata=sarg, lpattern }
     return $ if negated then Unary { unop=Neg, arg=like } else like

sc _ P.Filter { P.oper } =
  Left $ E.unexpected "operator" oper

sc _ s_ = Left $ E.unexpected "scalar operator" s_

{- converts a list into ANDs -}
conjunction :: Config -> [P.Expr] -> Either String ScalarExpr
conjunction config exprs =
  do solved <- mapM ((sc config ). P.expr) exprs
     case solved of
       [] -> Left $ E.unexpected  "empty conjunction list" exprs
       [x] -> return x
       x : y : rest -> return $ foldl' makeAnd (makeAnd x y) rest
       where makeAnd a b = Binop { binop=LogAnd, left=a, right=b }


readIntLiteral :: B.ByteString -> Integer
readIntLiteral str =
  case (C.readInteger str :: Maybe (Integer, B.ByteString)) of
    Just (num, "") -> num
    _ -> error $  "unrecognizable integer literal: " ++ (C.unpack str)

mplanFromParseTree :: P.Rel -> Config -> Either String RelExpr
mplanFromParseTree _1 config  = solve config _1

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
                , joinvariant=joinvariant@Plain
                } =
      Just $ Select { child=Join { leftch
                                 , rightch = child
                                 , conds=cond :| []
                                 , joinvariant
                                 }
                    , predicate }
    -- fact table selects get pushed up as well, after dim table
    -- NOTE: the predicate merge step is meant to map bottommost to leftmost
    swap Join { leftch = Select { child=selectchild -- push left select
                                   , predicate }
                  , rightch
                  , conds=cond :| []
                  , joinvariant=joinvariant@Plain
                  } =
      Just $ Select { child=Join { leftch = selectchild
                                     , rightch
                                     , conds=cond :| []
                                     , joinvariant
                                     }
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
