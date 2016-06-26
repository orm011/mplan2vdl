module Sha(sha1, sha1hack, SHA1) where

import qualified Crypto.Hash as H
import qualified Data.ByteString.Lazy.Char8 as C
import Data.Hashable

type SHA1 = H.Digest H.SHA1

sha1 :: C.ByteString -> SHA1
sha1 bs = H.hashlazy bs

-- hack: this case uses the class Hashable machinery to simplify dealing with multiple type variants. Try to use more than one salt to not be as weak as the normal hash
sha1hack :: (Hashable a) => a -> SHA1
sha1hack a =
  sha1
  $ C.append (C.pack $ show (hashWithSalt 0xabc a))
  $ C.append (C.pack $ show (hashWithSalt 0x357 a))
  $ C.pack $ show (hashWithSalt 0xdeadbeef a)
