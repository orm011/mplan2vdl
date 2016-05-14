import System.Environment
import qualified Data.List as List
import qualified Data.Maybe as Maybe
import Data.Either()
import Text.Groom
import System.IO

(|>) :: a -> (a -> b) -> b
(|>) f g = g f

main :: IO ()
main = do a <- getArgs
          case a of
            [filename] ->
              do contents <- readFile filename
                 putStrLn "input:" >> putStrLn contents >> putStrLn ""
            _ -> error "we need an input file name with a query in it"
