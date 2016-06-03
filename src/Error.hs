module Error (unexpected) where

--import Text.Groom
import Text.Printf (printf)

unexpected :: (Show a) => String -> a -> String
unexpected str a = printf "unexpected %s %s" str (take 50 $ show a)
