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
            ) where

import qualified Parser as P
import Types
import Name(Name(..),TypeSpec(..),lookup_err)
import Control.Monad.Reader
import Data.Time.Calendar
import Control.DeepSeq(NFData)
import Control.Exception.Base(assert)
import GHC.Generics (Generic)
import Data.Hashable
import Debug.Trace
import Data.Time.Format
import Data.Data
import Data.Generics.Uniplate.Data
import Config
import qualified Error as E
import Data.List(foldl')
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C
import Data.List.NonEmpty(NonEmpty(..))
import qualified Data.HashMap.Strict as HashMap

isJoinIdx :: P.Attr -> [Name]
isJoinIdx (P.JoinIdx s) = [s]
isJoinIdx _ = []

getJoinIdx :: [P.Attr] -> [Name]
getJoinIdx attrs = foldl' (++) [] (map isJoinIdx attrs)

resolveCharLiteral  :: Name -> Config -> B.ByteString -> Integer
resolveCharLiteral _ config ch = case HashMap.lookup ch (dictionary config) of
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

resolveInfix :: B.ByteString -> BinaryOp
resolveInfix str =
  case str of
    "<" -> Lt
    ">" -> Gt
    "<=" ->  Leq
    ">=" ->  Geq
    "=" ->  Eq
    "!=" ->  Neq
    "or" ->  LogOr
    _ -> error $ E.unexpected "infix symbol" str


resolveBinopOpcode :: Name -> BinaryOp
resolveBinopOpcode nm =
  case nm of
    Name ["sql_add"] ->  Add
    Name ["sql_sub"] ->  Sub
    Name ["sql_mul"] ->  Mul
    Name ["sql_div"] ->  Div
    Name ["sql_min"] ->  Min
    Name ["sql_max"] ->  Max
    Name ["="] ->  Eq
    Name ["or"] ->  LogOr
    Name ["and"] ->  LogAnd
    Name [">"] ->  Gt
    Name ["<>"] ->  Neq
    Name ["scale_down"] ->  Div
    _ -> error $ E.unexpected "binary function" nm


  {- they must be semantically for a single tuple (see aggregates otherwise) -}
data UnaryOp =
  Neg | Year | IsNull
  deriving (Eq, Show, Generic, Data)
instance NFData UnaryOp

resolveUnopOpcode :: Name -> UnaryOp
resolveUnopOpcode nm =
  case nm of
    Name ["year"] ->  Year
    Name ["sql_neg"] ->  Neg
    Name ["isnull"] ->  IsNull
    _ -> error $ E.unexpected "scalar function " nm

 {- a Ref can be a column or a previously bound name for an intermediate -}
data ScalarExpr =
  Ref Name
  | Literal DType Integer -- the integer encodes the representation of the value.
  | Identity { e::ScalarExpr } -- returns a rowid.
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

solveGroupOutput :: Config -> P.Expr -> (GroupAgg, Maybe Name)

solveGroupOutput _ P.Expr
  { P.expr = P.Ref {P.rname} {- output key -}
  , P.alias }
  = let outname = case alias of
          Nothing -> Just rname
          Just _ -> alias
    in (GFold FChoose (Ref rname), outname)


-- this is count(*). counts everything. equal to a sum of 1s.
solveGroupOutput _ P.Expr
  { P.expr = P.Call { P.fname=Name ["count"]
                    , P.args=[] }
  , P.alias }
  = (GCount, alias)

solveGroupOutput config P.Expr
  { P.expr = P.Call {  P.fname
                     , P.args=[ P.Expr
                                { P.expr=singlearg,
                                  P.alias = _ -- ignoring this inner alias.
                                }
                              ]
                     }
  , P.alias }
  = let inner = rsc config singlearg
    in case fname of
  Name ["sum"] -> (GFold FSum inner, alias)
  Name ["avg"] -> (GAvg inner, alias)
  Name ["max"] -> (GFold FMax inner, alias)
  Name ["min"] -> (GFold FMin inner, alias)
  Name ["count"]
    | P.Ref _ _ <- singlearg -> (GCount, alias)
  _ -> error $ E.unexpected  "unary aggregate" fname

-- this is count(col). counts only non null...
-- For now, deal with counting this way, which is incorrect.
-- In truth, We would need to have a separate vector to know which entries of this vector are null.
-- the only query using this (q 16?) seems to have it be not-null anyway.
-- monet does not seem to give an answer if the inner expression is not a column name.

solveGroupOutput  _ s_ = error $ E.unexpected "group_by output expression" s_

rsc :: Config -> P.ScalarExpr -> ScalarExpr
rsc config exp = runReader (sc exp) (Context {conf=config,dt=Nothing})


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
  | CartesianProduct { leftch :: RelExpr
                     , rightch :: RelExpr }
  | TopN      { child :: RelExpr
              , n :: Integer
              }
  deriving (Eq,Show, Generic, Data)
instance NFData RelExpr


{-helper function uesd in multiple operators that introduce output
columns -}
solveOutputs :: Config -> [P.Expr] -> [(ScalarExpr, Maybe Name)]
solveOutputs config explist =
  let f P.Expr { P.expr, P.alias } = (rsc config expr, alias)
  in map f explist

solve :: Config -> P.Rel -> RelExpr

{- Leaf (aka Table) invariants /checks:
  -tablecolumns must not be empty.
  -table columns must only be references, not complex expressions nor literals.
  -table columns may themselves be aliased within table.
  -some of the names involve using schema (not for now)
   for concrete resolution to things like partsupp.%partsupp_fk1
-}
solve _ P.Leaf { P.source, P.columns  } =
  let pcols = map split columns
  in assert (pcols /= []) $ Table { tablename = source, tablecolumns = pcols}
  where
    split P.Expr { P.expr = P.Ref { P.rname, P.attrs } --no alias. then use attr or name
                 , P.alias=Nothing } =
      case getJoinIdx attrs of
        [fkcol] -> (fkcol, Just rname) -- notice reversal
        [] -> (rname, Nothing)
        s_ -> error $ E.unexpected " multiple fkey indices" s_
    split P.Expr { P.expr = P.Ref { P.rname, P.attrs }
                 , P.alias=as@(Just _) } =
      case getJoinIdx attrs of
        [] -> (rname, as) --
        [fkcol] ->  (fkcol, as)  -- here, we have both an alias and joinidx (eg q8 nation_fk1)
        _ -> error $ E.unexpected " multiple fkey indices" attrs
    split _ = error "table outputs should only have reference expressions"

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
  let child = solve config ch
      projectout = solveOutputs config out
      order = (case rest of
                 [] -> []
                 _ -> error $ E.unexpected "order-by clauses" rest)
  in  Project {child, projectout, order }


  {- Group invariants:
     - single child node
     - multiple output value columns with potential expressions (non-empty)
     - multiple group key value columns (non-empty)
  -}
solve config P.Node { P.relop = "group by"
                    , P.children = [ch] -- only one child rel allowed for group by
                    , P.arg_lists =  igroupkeys : igroupvalues : []
                    } =
  let child = solve config ch
      inputkeys = map extractKey igroupkeys
      outputaggs = map (solveGroupOutput config) igroupvalues
  in GroupBy {child, inputkeys, outputaggs}
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
  let child = solve config ch
      predicate = conjunction config props
  in Select { child, predicate }


solve config arg@P.Node { P.relop
                        , P.children = [l, r]
                        , P.arg_lists =
                          conditions:[]
                          {- remember : its a list of lists -}
                        }
  | ("semijoin" == relop || "join" == relop || "antijoin" == relop || "left outer join" == relop) =
    case (cross_product config, relop) of
      (True, "join") -> -- not currently used for semijoin, antijoin, left outer.
        let cross = CartesianProduct {leftch=solve config l, rightch=solve config r}
        in Select {child=cross, predicate = conjunction config conditions}
      _ -> classify_join config arg -- revert to older code for other joins.

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
  let n = readIntLiteral stringRep
      child = solve config ch
  in TopN { child , n }

solve _ P.Node { P.relop } = error $ E.unexpected "relational operator not implemented" relop

classify_join :: Config -> P.Rel -> RelExpr
classify_join config P.Node { P.relop
                            , P.children = [l, r]
                            , P.arg_lists =
                              conditions:[]
                 {- remember : its a list of lists -}
                    }
  | ("semijoin" == relop || "join" == relop || "antijoin" == relop || "left outer join" == relop) =
  let leftch  = solve config l
      rightch = solve config r
      joinvariant = case relop of
             "join" -> Plain
             "semijoin" -> LeftSemi
             "antijoin" -> LeftAnti
             "left outer join" -> LeftOuter
             _ -> error $ "no other joins expected: "  ++ C.unpack relop
      prelim_conds = map ( (rsc config) . P.expr) conditions
      conds = case prelim_conds of
             [] -> error " empty join condition list is invalid "
             first : rest -> first :| rest
   in Join { leftch, rightch, conds, joinvariant }

classify_join _ _ = error "only meant for joins"

{- code to transform parser scalar sublanguage into Mplan scalar -}
data Context = Context {conf::Config, dt::Maybe DType}

sc :: P.ScalarExpr -> Reader Context ScalarExpr
sc P.Ref { P.rname  } = return $ Ref rname

-- reweites things like
-- sys.sql_add(date "1996-01-01", month_interval "3")
-- sys.sql_sub(date "1998-12-01", sec_interval(4) "7776000000") that show up in tpch mplans.
-- into a date literal that then gets handled like other plain date literals
sc P.Call { P.fname = Name [op]
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
         newexpr = P.Literal {P.tspec=TypeSpec {tname="date", tparams=[]}, P.stringRep}
     in sc newexpr

-- TODO: doublecheck this is correct
-- (see query 17 and query 20 for an example plan using it)
sc P.Call { P.fname=Name ["identity"]
          , P.args = [ P.Expr { P.expr } ]
          } = -- rewrite into just inner expression
  do e <- sc expr
     return $ Identity { e }

-- one of two versions of like in the plans:
sc P.Call { P.fname=Name ["like"]
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
  do ldata <- sc likearg
     return $ Like { ldata, lpattern=likepattern }

sc P.Call { P.fname=Name ["like"] } = error "implement this 'like' case"


{- for now, we are ignoring the aliases within calls -}
sc P.Call { P.fname, P.args = [ P.Expr { P.expr = singlearg, P.alias = _ } ] } =
  do sub <- sc singlearg
     let unop = resolveUnopOpcode fname
     return $ Unary { unop, arg=sub }


sc P.Call { P.fname
          , P.args = [ P.Expr { P.expr = firstarg, P.alias = _ }
                     , P.Expr { P.expr = secondarg, P.alias = _ }
                     ]
          } =
  do config <- asks conf
     left <- sc firstarg
     let newdt = case left of
           Ref n -> Just $ fst $ dtype $ snd $ Name.lookup_err n (colinfo config)
           _ -> Nothing
     right <- local (\ctx -> ctx {dt=newdt}) (sc secondarg) -- sys.= is a function .
     let binop = resolveBinopOpcode fname
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
  do context_type <- asks dt
     cfg <- asks conf
     let mtype = resolveTypeSpec tspec
     let (dtype,repr) = case mtype of
           MDate -> (DDate, resolveDateString stringRep)
           MDecimal _ b -> (DDecimal{point=fromInteger b}, readIntLiteral stringRep)
           -- sql 0.06 shows up
           -- as mplan Decimal (,2) "6", so just reinterpret int as decimal.
           MBoolean  -> (case stringRep of
                            "true" -> (DDecimal{point=0}, 1)
                            "false" -> (DDecimal{point=0}, 0)
                            _ -> error $ "unknown invalid boolean literal: " ++ show arg)
           MTinyint -> (DDecimal{point=0}, readIntLiteral stringRep)
           MSmallint -> (DDecimal{point=0}, readIntLiteral stringRep)
           MInt -> (DDecimal{point=0}, readIntLiteral stringRep)
           MBigInt -> (DDecimal{point=0}, readIntLiteral stringRep)
           -- the problem here is that we don't really know the bitwidth...
           -- so assume the worst (likely it won't match columns it is used against)
           MChar _ -> case context_type of
             Just (t@DString { decoder }) -> (t, resolveCharLiteral decoder cfg stringRep)
             _ -> error $ "need more information to assign type to char literal: " ++ show context_type ++ show arg
           _ -> error $ "unexpected literal: " ++ show arg
     return $ Literal dtype repr

sc P.Infix {P.infixop
           ,P.left
           ,P.right} =
  do config <- asks conf
     l <- sc (P.expr left)
     let newdt = case l of
           Ref n -> Just $ fst $ dtype $ snd $ Name.lookup_err n (colinfo config)
           _ -> Nothing
     r <- local (\ctx -> ctx {dt=newdt}) $ sc (P.expr right)
     let binop = resolveInfix infixop
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
     let fop = resolveInfix firstop
     let sop = resolveInfix secondop
     let left = Binop { binop=fop, left=sfirst, right=smiddle }
     let right = Binop { binop=sop, left=smiddle, right=slast }
     return $ Binop { binop=LogAnd, left, right}


sc s@(P.In { P.arg = P.Expr { P.expr = e@P.Ref {P.rname}, P.alias = _}
           , P.negated = False
           , P.set }) =
  do (left_dtype,_)  <- traceShow s $ asks $ dtype . snd . (Name.lookup_err rname) . colinfo . conf
     sleft <-  traceShow left_dtype $ sc e
     sset <- sequence $ map ((local (\ctx -> ctx {dt=Just left_dtype})) . sc . P.expr) set
     return $ In { left=sleft, set=sset }

sc s@(P.In {}) = error $ "implement this case of IN operator" ++ show s

sc (P.Nested exprs) =
  do cf <- asks conf
     return $ conjunction cf exprs

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

sc P.Filter { P.oper } = error $ E.unexpected "operator" oper

sc s_ = error $ E.unexpected "scalar operator" s_

{- converts a list into ANDs -}
conjunction :: Config -> [P.Expr] -> ScalarExpr
conjunction config exprs =
  let solved = map ((rsc config ). P.expr) exprs
      makeAnd a b = Binop { binop=LogAnd, left=a, right=b }
  in case solved of
        [] -> error $ E.unexpected  "empty conjunction list" exprs
        [x] -> x
        x : y : rest -> foldl' makeAnd (makeAnd x y) rest

readIntLiteral :: B.ByteString -> Integer
readIntLiteral str =
  case (C.readInteger str :: Maybe (Integer, B.ByteString)) of
    Just (num, "") -> num
    _ -> error $  "unrecognizable integer literal: " ++ (C.unpack str)

mplanFromParseTree :: P.Rel -> Config -> RelExpr
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
