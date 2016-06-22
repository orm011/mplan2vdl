module Error (unexpected,todo,check,Err,errzip) where

--import Text.Groom
import Text.Printf (printf)

unexpected :: (Show a) => String -> a -> String
unexpected str a = printf "unexpected %s %s" str (take 500 $ show a)

todo :: (Show a) => String -> a -> String
todo str a = printf "TODO %s %s" str (take 50 $ show a)

-- this way to insert extra checks
check :: (Show a) => a -> (a -> Bool) -> String -> Either String ()
check val cond msg = if cond val then Right () else Left $ unexpected msg val

type Err a = Either String a

errzip :: Err a -> Err b -> Err (a,b)
errzip (Left m)  _ = Left m
errzip _  (Left m) = Left m
errzip (Right a) (Right b) = Right (a,b)
