module Vlite( vexpsFromMplan
            , Vexp(..)
            , Vx(..)
            , BinaryOp(..)
            , ShOp(..)
            , FoldOp(..)) where

import Config
import qualified Mplan as M
import Mplan(BinaryOp(..))
import Name(Name(..))
import qualified Name as NameTable
import Control.Monad(foldM)
import Data.List (foldl')
import Prelude hiding (lookup) {- confuses with Map.lookup -}
import GHC.Generics
import Control.DeepSeq(NFData)
import Data.Int
import qualified Error as E
import Error(errzip,check)
import Data.Bits
--import Debug.Trace
import Text.Groom
--import qualified Data.Map.Strict as Map
--import Data.String.Utils(join)
import Data.List.NonEmpty(NonEmpty(..))
--type Map = Map.Map
--type VexpTable = Map Vexp Vexp  --used to dedup Vexps
import Text.Printf
import Control.Exception.Base hiding (mask)
import qualified Data.Map.Strict as Map

--type Map = Map.Map
type NameTable = NameTable.NameTable

data ShOp = Gather | Scatter
  deriving (Eq, Show, Generic,Ord)
instance NFData ShOp

data FoldOp = FSum | FMax | FMin | FSel
  deriving (Eq, Show, Generic, Ord)
instance NFData FoldOp

data Vx =
  Load Name
  | RangeV { rmin :: Integer, rstep :: Integer, rref::Vexp }
  | RangeC { rmin :: Integer, rstep :: Integer, rcount::Integer }
  | Binop { binop :: BinaryOp, left :: Vexp, right :: Vexp }
  | Shuffle { shop :: ShOp, shsource :: Vexp, shpos :: Vexp }
  | Fold { foldop :: FoldOp, fgroups :: Vexp, fdata :: Vexp }
  | Partition { pivots:: Vexp, pdata::Vexp }
  deriving (Eq,Show,Generic)
instance NFData Vx

data UniqueSpec = Unique | Any deriving (Eq,Show,Generic)
instance NFData UniqueSpec

data Lineage = Pure {col::Name, mask::Vexp, quant::UniqueSpec} | None  deriving (Eq,Show,Generic)
instance NFData Lineage

data Vexp = Vexp { vx::Vx
                 , info::ColInfo
                 , lineage::Lineage
                 , name::Maybe Name } deriving (Eq,Show,Generic)

-- if lineage is set to somthing, it means the
-- column values all come untouched from an actual table column (as opposed to being derivative)
-- however, the values may have been shuffled/duplicated in different ways. the Vexp shows if this is so.
-- basically, the following identity holds: vx = gather gathermask=lineagevx data=tableName
-- if the uniqueSpec is unique, then it means that the column has not duplicated values wrt
-- the original one.
-- Note also: right now, uniqueSpec means conditionally unique: if the original column was unique (such as the pos id)
-- then the current version is also unique.

instance NFData Vexp

-- instance Monad W where
--   W vx  >>= fn = fn vx
--   return a = W a
--- Note: if I want vectors to keep their names, then all of these functions
--- that make a new vector need to have names for their inner vectors.

{- some convenience vectors -}
pos_ :: Vexp -> Vexp
pos_ v = complete $ RangeV {rmin=0, rstep=1, rref=v}

const_ :: Integer -> Vexp -> Vexp
const_ k v = complete $ RangeV{rmin=k, rstep=0, rref=v}

zeros_ :: Vexp -> Vexp
zeros_ = const_ 0

ones_ :: Vexp -> Vexp
ones_ = const_ 1

(==.) :: Vexp -> Vexp -> Vexp
a ==. b = makeBinop Eq a b

(>>.) :: Vexp -> Vexp -> Vexp
a >>. b = makeBinop BitShift a b

(<<.) :: Vexp -> Vexp -> Vexp -- BitShift uses sign to encode direction
a <<. b = let z = zeros_ b
              negb = (z -. b)
          in a >>. negb

(||.) :: Vexp -> Vexp -> Vexp
a ||. b = makeBinop LogOr a b

(|.) :: Vexp -> Vexp -> Vexp
a |. b = makeBinop BitOr a b

(&.) :: Vexp -> Vexp -> Vexp
a &. b = makeBinop BitAnd a b

(-.) :: Vexp -> Vexp -> Vexp
a -. b = makeBinop Sub a b

(*.) :: Vexp -> Vexp -> Vexp
a *. b = makeBinop Mul a b

(+.) :: Vexp -> Vexp -> Vexp
a +. b = makeBinop Add a b

(/.) :: Vexp -> Vexp -> Vexp
a /. b = makeBinop Div a b

makeBinop :: BinaryOp -> Vexp -> Vexp -> Vexp
makeBinop binop left right = complete $ Binop {binop, left, right}

(?.) :: Vexp -> (Vexp,Vexp) -> Vexp
cond ?. (a,b) =  let  ones = ones_ cond
                      zeros = zeros_ cond
                      negcond = cond ==. zeros
                      -- need to make condition boolean for mult.
                      poscond = ones -. negcond
                      left = poscond *. a
                      right = negcond *. b
                 in left +. right

complete :: Vx -> Vexp
complete vx =
  let info = checkColInfo $ inferMetadata vx
      lineage = checkLineage $ inferLineage vx
  in Vexp { vx, info, lineage, name=Nothing }

checkLineage :: Lineage -> Lineage
checkLineage l =
  case l of
    None -> None
    Pure  _ Vexp {lineage, name} _ ->
      case (lineage,name) of
        (None, Nothing) -> l
        _ -> error "lineage vector should not itself have lineage or name"

inferMetadata :: Vx -> ColInfo

inferMetadata (Load _) = error "at the moment, should not be called with Load. TODO: need to pass config to address this case"

inferMetadata RangeV {rmin=rstart,rstep,rref=Vexp {info=ColInfo {count}}}
  =  let extremes = [rstart, rstart + count*rstep]
     in ColInfo { bounds=(minimum extremes, maximum extremes)
                , count }

inferMetadata RangeC {rmin=rstart, rstep, rcount}
  =  let extremes = [rstart + rcount*rstep, rstart]
     in ColInfo { bounds=(minimum extremes, maximum extremes)
                , count=rcount }

inferMetadata Shuffle { shop=Scatter
                      , shsource=Vexp {info=ColInfo {bounds=sourcebounds}}
                      , shpos=Vexp {info=ColInfo {bounds=(_,posmax)}}
                      }
  = ColInfo { bounds=sourcebounds, count=posmax }

inferMetadata Shuffle { shop=Gather
                      , shsource=Vexp {info=ColInfo {bounds=sourcebounds}}
                      , shpos=Vexp {info=ColInfo {count}}
                      }
  = ColInfo { bounds=sourcebounds, count }

inferMetadata Fold { foldop=FSel
                   , fgroups=_
                   , fdata=Vexp {info=ColInfo {count}}
                   }
  = ColInfo {bounds=(0, count-1), count} -- coefficients

inferMetadata Fold { foldop
                   , fgroups = Vexp { info=ColInfo {count=gcount} }
                   , fdata = Vexp { info=ColInfo {bounds=(dlower,dupper), count=dcount} }
                   } =
  -- "suspicious: group and data count bounds dont match"
  assert (gcount == dcount) $
  case foldop of
    FSum -> let extremes = [dlower, dlower*dcount, dupper, dupper*dcount]
                  -- for positive dlower, dlower is the minimum.
                  -- for negative dlower, dlower*dcount is the minimum, and so on.
            in ColInfo (minimum extremes, maximum extremes) dcount
    FMax -> ColInfo (dlower, dupper) dcount
    FMin -> ColInfo (dlower, dupper) dcount
    FSel -> error "use different handler for select"

-- the result of partition is a list of indices that, for each pdata
-- tells it where the first element in pivots is that is larger or equal to it.
-- so, the outputsize is the same as that of pdata
-- and, if there is a bound for each element in the input,
-- the output values are in the domain [0..(count pivots  - 1)]
inferMetadata Partition
  { pivots=Vexp { info=ColInfo {count=pivotcount} }
  , pdata=Vexp { info=ColInfo {count=datacount} }
  } = ColInfo {bounds=(0,pivotcount-1), count=datacount}


inferMetadata Binop
  { binop
  , left=left@Vexp { info=ColInfo {bounds=(l1,u1), count=c1 } }
  , right=right@Vexp { info=ColInfo {bounds=(l2,u2), count=c2 } }
  } = do
         let count = min c1 c2
             bounds = case binop of
                Gt ->  (0,1)
                Lt ->  (0,1)
                Eq ->  (0,1)
                Neq ->  (0,1)
                Geq ->  (0,1)
                Leq ->  (0,1)
                LogAnd ->  (0,1)
                LogOr ->  (0,1)
                Add ->  (l1 + l2, u1 + u2)
                Sub ->  (l1 - u2, u1 - l2) -- notice swap
                Mul -> let allpairs = sequence [[l1,u1],[l2,u2]] -- cross product
                           prods = map (foldl (*) 1) allpairs
                       in  (minimum prods, maximum prods) -- TODO double check reasoning here.
                Div -> let allpairs = [(l1,l2), (l1,u2), (u1,l2), (u1,u2)]
                           divs = map (\(x,y) -> x `div` y) allpairs
                       in  (minimum divs, maximum divs) -- TODO double check reasoning here.
                Min -> (min l1 l2, min u1 u2) -- this is true, right?
                Max -> (max l1 l2, max u1 u2)
                Mod -> (0, u2) -- assuming mods are always positive
                BitAnd -> if (l1 >= 0 && l2 >= 0 && u1 < (1 `shiftL` 31) && u2 < (1 `shiftL` 31))
                          then let mx = min (maxForWidth left) (maxForWidth right)
                               in (0,mx) -- precision could be improved.
                          else error $ E.todo "cant deduce BitAnd bounds" ((l1,u1),(l2,u2))
                BitOr -> if (l1 >= 0 && l2 >= 0 && u1 < (1 `shiftL` 31) && u2 < (1 `shiftL` 31))
                         then let mx = max (maxForWidth left) (maxForWidth right)
                              in (0,mx) -- precision could be improved.
                         else error $ E.todo "cant deduce BitOr bounds" ((l1,u1),(l2,u2))
                BitShift -> assert (u2 < 32) $ --"shift left by more than 31?"
                            assert (l2 > -32) $ --"shift right by more than 31?"
                               -- shift left is like multiplication (amplifies neg and pos)
                               -- shift right is like division (shrinks numbers neg and pos)
                               -- both can happen in a single call to shift...
                            let mshift (a,b) = let shfmask = (fromInteger $ toInteger $ abs b)
                                               in if b < 0 then (a `shiftR` shfmask)
                                                  else a `shiftL` shfmask
                                allpairs = [(l1,l2), (l1,u2), (u1,l2), (u1,u2)]
                                extremes = map mshift allpairs
                            in (minimum extremes, maximum extremes)
           in ColInfo {bounds, count}

inferLineage :: Vx -> Lineage

inferLineage Shuffle { shop=Scatter --- note: we are assuming the scatter covers everything.
                     , shsource=Vexp {lineage}
                     , shpos }
 | Pure {col, mask=lineagev, quant} <- lineage  =
     Pure  {col
           , mask=complete $ Shuffle { shop=Scatter
                                     , shsource=lineagev
                                     , shpos
                                     }
           , quant }  -- scatter does not duplicate values

inferLineage Shuffle { shop=Gather
                     , shsource=Vexp {lineage}
                     , shpos }
  | Pure {col, mask=lineagev} <- lineage  =
      Pure { col
           , mask=complete $ Shuffle { shop=Gather
                                     , shsource=lineagev
                                     , shpos
                                     }
           , quant=Any } -- gather can duplicate values.

inferLineage Fold { foldop
                   , fgroups
                   , fdata = Vexp { lineage }
                   }
  | (foldop == FMin) || (foldop == FMax)
  , Pure  {col, mask=lineagev, quant} <- lineage =
      Pure {col, mask=complete $ Fold {foldop, fgroups, fdata=lineagev}, quant}
       --Foldmin, max also preserve

inferLineage _ = None


vexpsFromMplan :: M.RelExpr -> Config -> Either String [Vexp]
vexpsFromMplan r c  = solve' c r

outputName :: (M.ScalarExpr, Maybe Name) -> Maybe Name
outputName (_, Just alias) = Just alias {-alias always wins-}
outputName ((M.Ref orig), Nothing) = Just orig {-anon still has name if ref-}
outputName _ = Nothing

-- includes both the list of all entries, as well
-- as a lookup friendly map
data Env = Env [Vexp] (NameTable Vexp)

makeEnv :: [Vexp] -> Either String Env
makeEnv lst =
  do tbl <- makeTable lst
     return $ Env lst tbl
  where makeTable pairs = foldM maybeadd NameTable.empty pairs
        maybeadd env Vexp { name=Nothing } = Right env
        maybeadd env vexp@Vexp { name=(Just newalias)} =
          NameTable.insert newalias vexp env


makeEnvWeak :: [Vexp] -> Env
makeEnvWeak lst =
  let tbl = makeTable lst
  in Env lst tbl
  where makeTable pairs = foldl' maybeadd NameTable.empty pairs
        maybeadd env Vexp { name=Nothing} = env
        maybeadd env vexp@Vexp { name=(Just newalias) } =
          NameTable.insertWeak newalias vexp env

{- helper function that makes a lookup table from the output of a previous
operator. the lookup table loses information about the anonymous outputs
of the operator, so it only makes sense to use this in internal nodes
whose vectors will be consumed by other operators, but not on the
top level operator, which may return anonymous columns which we will need to
keep around -}
solve :: Config -> M.RelExpr -> Either String Env
solve config relexp = (solve' config relexp) >>= makeEnv

solve' :: Config -> M.RelExpr -> Either String [Vexp]

{- Table is a Special case. It gets vexprs for all the output columns.

If the output columns are not aliased, we can always
use their original names as output names. (ie, there are no
anonymous expressions in the list)

note: not especially dealing with % names right now
todo: using the table schema we can resolve % names before
      they get to the final voodoo
-}
solve' config M.Table { M.tablename=_
                      , M.tablecolumns } =
  let tidcols = filter isTid tablecolumns
      nontid = filter (not . isTid) tablecolumns
      nontidloads =
        sequence $ (do col@(colnam, _) <- nontid -- list monad
                       let alias = Just $ decideAlias col
                       return $ (do (_,info) <- NameTable.lookup colnam (colinfo config)  -- either monad
                                    let vx=Load colnam
                                    let partial=Vexp {vx, info, lineage=None, name=Nothing} -- this is just for type compatiblity.
                                    let lineage=Pure{col=colnam, mask=pos_ partial, quant=Unique} -- TODO: use a single vector for lineage range (eg primary key)
                                    let completed = partial {lineage=lineage, name=alias}
                                    return $ completed))
  in do snontids@(refv@Vexp { }: _)  <- nontidloads --either
        case tidcols of
          [] -> return $ snontids
          [tidcol@(orig,_)] ->
            let anon = pos_ refv
                lineage = Pure {col=orig, mask=pos_ refv, quant=Unique} -- lineage of itself.
                named = anon { lineage, name=Just $ decideAlias tidcol }
            in return $ named : snontids
          _ -> Left $ E.unexpected "multiple tidcols defined in table" tidcols
  where decideAlias (orig, Nothing) = orig
        decideAlias (_, Just x) = x
        isTid (Name [_, "%TID%"], _) = True
        isTid (Name [_, _], _) = False
        isTid _ = undefined -- this should never happen. used here for warning.

        ---not expecting names with more than two components

{- Project: not dealing with ordered queries right now
note. project affects the name scope in the following ways

There are four cases illustrated below:
project (...) [ foo, bar as bar2, sum(bar, baz) as sumbaz, sum(bar, bar) ]

For the consumer to be able to see all relevant columns, we need
to solve the cases like this:

Note: a name is within scope starting from its position in the output list,
ie, later expressions in the name list can refer to previous expressions
in the name list.  this isn't common ( but it happens in tpch plans )

1) despite not having an 'as' keyword, foo should produce
(Vexprfoo, Just foo)
2) for bar, the alias is explicit:
(Vexprbar, Just bar2)
3) for sum(bar, baz), the alias is also explicit. this is the same
as case 2 actually. (Vexpr sum(bar, baz), sum2)
4) sum(bar, bar) could not be referred to in a consumer query,
but could be the topmost result, so we must return it as well.
as (vexpr .., Nothing)
-}
solve' config M.Project { M.child, M.projectout, M.order = [] } =
  do (Env list0 _) <- solve config child -- either monad -- used for reading
     (_ , solved) <- foldM foldFun (list0,[]) projectout
     return $ solved
     where addEntry (list0, acclist) tup = (list0, tup : acclist) -- adds output to acclist
           foldFun lsts@(list0, acclist) arg@(expr, _) =
             do asenv <- return $ makeEnvWeak $ list0 ++ acclist-- use both lists for name resolution
                 -- allow collisions b/c they do happen .
                anon  <- fromScalar asenv expr --either monad
                return $ addEntry lsts $ anon {name=outputName arg}


-- problem:
-- n2.n_name as all_nations.nation defined then directly uesd in a subsequent expression. not found.
-- L2 as L2.L2 used to redefined L2.L2 from a sub-output (name clash on L2.L2) then not used
-- in the  same project list .
solve' config M.GroupBy { M.child,
                          M.inputkeys,
                          M.outputaggs } =
  do env@(Env (refv:_) nt)  <- solve config child --either monad
     lookedup  <- (mapM (\n -> (NameTable.lookup n nt)) inputkeys)
     let keyvecs = map snd lookedup
     let gbkeys = case keyvecs of
           [] -> zeros_ refv :| []
           a : rest -> a :| rest
     let gkey@Vexp { info=ColInfo {bounds=(gmin, _) } } = makeCompositeKey gbkeys
     assert (gmin == 0) $
       sequence $ (do pr@(agg, _) <- outputaggs -- list monad
                      return ( do anon  <- solveAgg config env gkey agg
                                  let outalias = case pr of
                                        (M.GDominated n, Nothing) -> Just n
                                        (_, alias) -> alias
                                        _ -> Nothing
                                  return $ anon {name=outalias}))


solve' config M.EquiJoin { M.leftch
                         , M.rightch
                         , M.cond=(key1, key2)
                         } =
  do Env leftcols leftenv  <- solve config leftch
     Env rightcols rightenv <- solve config rightch
     --- find the keys in the subrelations. the order is not well defined,
     --- so, try both and check if there was ambiguity.
     let tryOne =
           errzip (NameTable.lookup key1 leftenv) (NameTable.lookup key2 rightenv)
     let tryTwo =
           errzip (NameTable.lookup key1 rightenv) (NameTable.lookup key2 leftenv)
     ((skey1,cols1), (skey2,cols2)) <- case (tryOne,tryTwo) of
       (Left _, Left _) -> Left "failed join lookup"
       (Right ((_,key1m),(_,key2m)), Left _) -> Right ((key1m, leftcols), (key2m, rightcols))
       (Left _, Right ((_,key1m),(_,key2m))) -> Right ((key1m, rightcols), (key2m, leftcols))
       (Right _, Right _) -> Left "ambiguous key lookup"
     --- check which way the reference goes
     let (colname1, idx1) = case skey1 of
           Vexp { lineage=Pure {col=a, mask=b} } -> (a,b)
           Vexp { lineage=None } -> error "no lineage available for join"
     let (colname2,idx2) = case skey2 of
           Vexp { lineage=Pure {col=a,mask=b}  } -> (a,b)
           Vexp { lineage=None } -> error "no lineage available for join"
     let (factcols, dimcols, (gatheridx, selectidx)) = ( -- select idx is a list of positions. --gatheridx is a mask that can be applied to cols after select
           if (colname1 == colname2)
              -- self join. look for version of table that is intact
           then case (idx2,idx1) of -- TODO handle case where both have been changed.
             (Vexp { vx=RangeV {rmin=0, rstep=1} },_) -> (cols1,cols2,(idx1, ones_ idx1)) -- use the idx1 as gather mask against cols2
             (_,Vexp { vx=RangeV {rmin=0, rstep=1} }) -> (cols2,cols1,(idx2, ones_ idx2))
             (_,_) -> error $ "both children of this self join have been modified.\n" ++ (take 50 $ show leftch) ++ "\n" ++ (take 50 $ show rightch)
           else case Map.lookup ((colname1,colname2):|[]) (fkrefs config) of
                      Nothing -> error "no fk constraint available for join"
                      Just ((fact_cname, dim_cname) :| [], fkidx)
                        | (fact_cname == colname1) && (dim_cname == colname2)
                          -> (cols1, cols2, deduceMasks idx1 idx2 fkidx)
                        | (fact_cname == colname2) && (dim_cname == colname1)
                          -> (cols2, cols1, deduceMasks idx2 idx1 fkidx)
                      _ -> error "multiple fk columns?")
     let cleaned_factcols = (do factcol@Vexp {name } <- factcols
                                let out_anon = complete $ Shuffle { shop=Gather
                                                                  , shsource=factcol
                                                                  , shpos=selectidx
                                                                  }
                                return $ out_anon {name=name}
                            )
     let joined_dimcols  = (do dimcol@Vexp { name } <- dimcols -- list monad
                               let joined_anon = complete $ Shuffle { shop=Gather
                                                                    , shsource=dimcol
                                                                    , shpos=gatheridx }
                               return $ joined_anon { name=name }
                           ) -- preserve the names
     return $ cleaned_factcols ++ joined_dimcols
       where deduceMasks _ dimcolv fkidx =
               let idxcol = Vexp { vx = Load fkidx
                                 , info = snd $ NameTable.lookup_err fkidx (colinfo config)
                                 , lineage = None
                                 , name =  Nothing }
                   gidx = complete $ Shuffle {shop=Gather, shpos=dimcolv, shsource=idxcol}
               in case dimcolv of
                 Vexp { vx=RangeV {rmin=0, rstep=1} } -> (gidx, ones_ gidx)
                 _ -> error $ "the dimension column has been modified:" ++ (show fkidx)

             -- topos datavec booleanvec =
             --   let shpos = complete $ Fold { foldop=FSel
             --                                , fgroups=pos_ boolean
             --                                , fdata = booleanvec
             --                                }
             --               datai


solve' config M.Select { M.child -- can be derived rel
                       , M.predicate
                       } =
  do childenv@(Env childcols _) <- solve config child -- either monad
     fdata  <- sc childenv predicate
     let fgroups = pos_ fdata
     let idx = complete $ Fold {foldop=FSel, fgroups, fdata}
     return $ do unfiltered@Vexp { name=preserved }  <- childcols -- list monad
                 return $ let sel = complete $ Shuffle { shop=Gather
                                                       , shsource=unfiltered
                                                       , shpos=idx }
                          in ( sel {name=preserved} )

solve' _ r_  = Left $ "unsupported M.rel:  " ++ groom r_

{- makes a vector from a scalar expression, given a context with existing
defintiions -}
fromScalar ::  Env -> M.ScalarExpr  -> Either String Vexp
fromScalar = sc

{- notes about tmp naming in Monet:
 -user columns are only named with lowercase.
 -temporary columns are sometimes named things like L.L or L1.L1.
 -to the right of 'as', we get fully qualified names
 -but sometimes, as a reference, they don't include the fully qualified names.
eg [L1 as L1]. This means we cannot just use a map in those cases, since
a search for L1 should potentially mean L1.L1.
-}
-- sc' :: Env -> M.ScalarExpr -> Either String Vexp
-- sc' env expr =
--   do vx <- sc env expr
--      return $ getInfo vx

sc ::  Env -> M.ScalarExpr -> Either String Vexp
sc (Env _ env) (M.Ref refname)  =
  case (NameTable.lookup refname env) of
    Right (_, v) -> Right v
    Left s -> Left s

-- serious TODO: strictly speaking, I need to know the orignal type in order
-- to correctly produce code here.
-- We need the input type bc, for example Decimal(10,2) -> Decimal(10,3) = *10
-- but Decimal(10,1) -> Decimal(10,3) = * 100. Right now, that input type is not explicitly given.
sc env (M.Cast { M.mtype, M.arg }) =
  case mtype of
    M.MTinyint -> sc env arg
    M.MInt -> sc env arg
    M.MBigInt -> sc env arg
    M.MSmallint -> sc env arg
    M.MChar -> sc env arg -- assuming the input has already been converted
    M.MDouble -> sc env arg -- assume it was an integer originally...
    M.MDecimal _ dec -> --we are assuming inputs are integers, but should not.
      do orig <- sc env arg
         -- multiply by 10^dec. hope there is no overflow.
         -- TODO now we could statically check for overflow using sizes
         let factor = (10 ^ dec)
         let fvec = const_ factor orig
         return $ orig *. fvec
    othertype -> Left $ "unsupported type cast: " ++ groom othertype


sc env (M.Binop { M.binop, M.left, M.right }) =
  do l <- sc env left
     r <- sc env right
     return $ complete $ Binop {binop, left=l, right=r}

sc env M.In { M.left, M.set } =
  do sleft  <- sc env left
     sset <- mapM (sc env) set
     let eqs = map (==. sleft) sset
     let (first,rest) = case eqs of
           [] -> error "list is empty here"
           a:b -> (a,b)
     return $ foldl' (||.) first rest

sc (Env (vref : _ ) _) (M.IntLiteral n)
  = return $ const_ n vref

sc env (M.Unary { M.unop=M.Year, M.arg }) =
  --assuming input is well formed and the column is an integer representing
  --a day count from 0000-01-01)
  do dateval <- sc env arg
     let v365 = const_ 365 dateval
     return $ dateval /. v365

--example use of isnull. In all the contexts of TPCH queries i saw, the isnull is called on
--a column or derived column that is statically known to not be null, so we just remove that.
{- sys.ifthenelse(sys.isnull(sys.=(all_nations.nation NOT NULL, char(25)[char(6) "BRAZIL"])), boolean "false", sys.=(all_nations.nation NOT NULL, char(25)[char(6) "BRAZIL"])) -}
sc env (M.IfThenElse { M.if_=M.Unary { M.unop=M.IsNull
                                     , M.arg=oper1 } , M.then_=M.IntLiteral 0, M.else_=oper2 } )=
  do check (oper1,oper2) (\(a,b) -> a == b) "different use of isnull than expected"
     sc env oper1 --just return the guarded operator.

-- note: for max and min, the actual possible bounds are more restrictive.
-- for now, I don't care.
sc env (M.IfThenElse { M.if_=mif_, M.then_=mthen_, M.else_=melse_ })=
  do if_ <- sc env mif_
     then_ <- sc env mthen_
     else_ <- sc env melse_
     return $ if_ ?.(then_,else_)

sc _ r = Left $ "(Vlite) unsupported M.scalar: " ++ (take 50  $ show  r)

solveAgg :: Config -> Env -> Vexp -> M.GroupAgg -> Either String Vexp

solveAgg _ (Env [] _) _ _  = Left $"empty input for group by"

-- average case dealt with by rewriting tp sum, count and finally using division.
solveAgg config env gkeyvec (M.GAvg expr) =
  do gsums@Vexp { info=ColInfo { count=cgsums } } <- solveAgg config env gkeyvec (M.GFold M.FSum expr)
     gcounts@Vexp { info=ColInfo { bounds=(minc,_ ), count=cgcounts} } <- solveAgg config env gkeyvec M.GCount
     check (cgsums,cgcounts) (\(a,b) -> a == b) "counts should match"
     check minc (== 1) $ "minimum count is always 1 but here it is " ++ show minc
     return $ gsums /. gcounts

-- all keys in a column dominated by groups are equal to their max
-- so deal with this as if it were a max
solveAgg config env gkeyvec (M.GDominated nm) =
  solveAgg config env gkeyvec (M.GFold M.FMax (M.Ref nm))

-- count is rewritten to summing a one for every row
solveAgg config env gkeyvec (M.GCount) =
  let rewrite = (M.GFold M.FSum (M.IntLiteral 1))
  in solveAgg config env gkeyvec rewrite

solveAgg config env gkeyvec (M.GFold op expr) =
  do scattermask <- getScatterMask gkeyvec
     let sortedGroups = complete $ Shuffle {shop=Scatter, shpos=scattermask, shsource=gkeyvec}
     gdata <- sc env expr
     let sortedData = complete $ Shuffle {shop=Scatter, shpos=scattermask, shsource=gdata}
     let foldop = case op of
           M.FSum -> FSum
           M.FMax -> FMax
     make2LevelFold config foldop sortedGroups sortedData

-- scatter mask for a group by
getScatterMask :: Vexp -> Either String Vexp
getScatterMask pdata@Vexp { info=ColInfo {bounds=(pdatamin, pdatamax)} } =
  if pdatamax == pdatamin 
  then return $ pos_ pdata -- pivots would be empty. unclear semantics.
  else let pivots = complete $ RangeC { rmin=pdatamin
                                 , rstep=1
                                 , rcount=pdatamax-pdatamin }
       in return $ complete $ Partition { pivots, pdata }

maxForWidth :: Vexp -> Integer
maxForWidth vec =
  let width = getBitWidth vec
      -- examples:
      -- bitwidth is 0, then max should be 0:  (1 << 0) - 1 = (1 - 1) = 0
      -- bitwidth is 1, then max should be 0b1. (1 << 1) - 1 = (2 - 1) = 1 = 0xb1
      -- bitwidth is 2, then max should be 0xb11. (1 << 2) -1 = (4 - 1) = 3 = 0xb11
      -- cannot really subtract 1 from 1 << 31, b/c is underflow. so just check.
  in assert (width < 32) $ --"about to shift by 32 or more"
     (1 `shiftL` (fromInteger width)) - 1

makeCompositeKey :: NonEmpty Vexp -> Vexp
makeCompositeKey (firstvexp :| rest) =
  let shifted = shiftToZero firstvexp -- needed bc empty list won't shift
      out = foldl' composeKeys shifted rest
      maxval = maxForWidth out
      maxvalV = const_ maxval out
  in  out &. maxvalV  --mask used as a hint to Voodoo (to infer size)

--- makes the vector min be at 0 if it isnt yet.
shiftToZero :: Vexp -> Vexp
shiftToZero arg@Vexp { info=ColInfo {bounds=(vmin,_)} }
  = if vmin == 0 then arg
    else let ret@Vexp { info=ColInfo {bounds=(newmin,_)} } = arg -. const_ vmin arg
         in assert (newmin == 0) ret

-- bitwidth required to represent all members
getBitWidth :: Vexp -> Integer
getBitWidth Vexp { info=ColInfo {bounds=(l,u)} }
  = max (bitsize l) (bitsize u)

bitsize :: Integer -> Integer
bitsize num =
  if num >= 0 then
    if num < toInteger (maxBound :: Int32) then
      let num32 = (fromInteger num) :: Int32
          ans = toInteger $ (finiteBitSize num32) - (countLeadingZeros num32)
      in assert (ans <= 31) ans
    else error (printf "number %d is larger than maxInt32" num)
  else error (printf "bitwidth only allowed for non-negative numbers (num=%d)" num)

composeKeys :: Vexp -> Vexp -> Vexp
composeKeys l r =
  let sleft = shiftToZero l
      sright = shiftToZero r
      oldbits = getBitWidth sleft
      deltabits = getBitWidth sright
      newbits = oldbits + deltabits
  in assert (newbits < 32) $
     (sleft <<. (const_  deltabits sleft)) |. sright

-- Assumes the fgroups are alrady sorted
make2LevelFold :: Config -> FoldOp -> Vexp -> Vexp -> Either String Vexp
make2LevelFold config foldop fgroups fdata =
  do let pos = pos_ fgroups
     let log_gsize = const_ (grainsizelg config) fgroups
     let ones = ones_ fgroups
     -- example: grainsize = 1. then log_gsize = 0. want 01010101... formula gives (pos >> 0) | 1 = 01010101...
     -- example: grainsize = 2. then log_gisze = 1. want 00110011... formulate gives (pos >> 1) | 1 = 00110011
     -- example: grainsize = 4. then log_gisze = 2. want 00001111... formulate gives (pos >> 2) | 1 ...
     let level1par = (pos >>. log_gsize) &. ones
     let level1groups = composeKeys fgroups level1par
     let level1results = complete $ Fold { foldop, fgroups=level1groups, fdata }
     let level2results = complete $ Fold { foldop, fgroups, fdata=level1results }
     return  $ assert (getBitWidth level1par == 1) level2results


