{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Exception
import Control.Monad (forever, void)
import Control.Monad.Trans (liftIO)
import qualified Data.List as List
import Data.Time
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import Network.Socket
import System.IO (hPutStrLn, stderr)
import System.Locale (defaultTimeLocale)

import Control.Lens
import qualified Data.Aeson as Aeson
import qualified Data.Attoparsec.ByteString as Atto
import qualified Data.Attoparsec.ByteString.Char8 as Atto8
import Data.Conduit
import Data.Conduit.Attoparsec (conduitParser)
import qualified Data.Conduit.List as Cl
import qualified Data.Conduit.Network.UDP as UDP
import Data.Conduit.Process (proc, sourceProcess)
import Data.Default (def)
import qualified Data.HashMap.Strict as HashMap
import Data.Serialize (runPut)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as Vector
import Options.Applicative hiding ((&))
import Options.Applicative.Types (ReadM(ReadM))

import Data.ProtocolBuffers (encodeMessage)
import qualified Network.Monitoring.Riemann as Riemann

hostPortReader :: String -> ReadM (String, Int)
hostPortReader x = ReadM $
  let (h, p) = List.span (/= ':') x
  in case reads (List.drop 1 p) of
       [(p', _)] -> Right (h, p')
       _         -> Left (ErrorMsg "Could not parse hostname")

opts :: ParserInfo ((String, Int), Int)
opts = info (helper <*> p) m where
  m = fullDesc
        <> progDesc "Turn ceph's health reports into Riemann events"
        <> header "Riemann.io client for Ceph"
  p = (,) <$> hp <*> d
  hp = nullOption (reader hostPortReader
         <> long "server"
         <> value ("127.0.0.1", 5555)
         <> help "Riemann server:port (default: \"127.0.0.1:5555\")")
  d = (1000000*) <$> option (long "delay"
         <> metavar "SECONDS"
         <> help "Delay between calls to 'ceph report'")

main :: IO ()
main = do
  ((host, port), delay) <- execParser opts
  hPutStrLn stderr $ "Sending reports to: " ++ host ++ ":" ++ show port

  let loop = do
        (s, AddrInfo{addrAddress = addr}) <- UDP.getSocket host port
        void . runResourceT $ forever (cephMon >> liftIO (threadDelay delay))
          =$= Cl.map (runPut . encodeMessage)
          =$= Cl.map (`UDP.Message` addr)
          $$ UDP.sinkToSocket s

  forever . catch loop $ \ e -> do
    hPutStrLn stderr $ "Failure: " ++ show (e :: SomeException)
    hPutStrLn stderr "Retrying after delay"
    threadDelay delay

-- "timestamp": "2013-03-09 22:47:03.171579",
parseJSONTime :: Text -> Maybe UTCTime
parseJSONTime = parseTime defaultTimeLocale "%Y-%m-%d %H:%M:%S%Q" . T.unpack

pgmapEvents :: Aeson.Object -> [Riemann.Event]
pgmapEvents obj
  | Just (Aeson.String ts)          <- HashMap.lookup "stamp" obj
  , Just ts'                        <- parseJSONTime ts
  , Just (Aeson.Object pgStatsSum)  <- HashMap.lookup "pg_stats_sum" obj
  , Just (Aeson.Object statsSum)    <- HashMap.lookup "stat_sum" pgStatsSum
  , Just (Aeson.Number objects)     <- HashMap.lookup "num_objects" statsSum
  , Just (Aeson.Number clones)      <- HashMap.lookup "num_object_clones" statsSum
  , Just (Aeson.Number copies)      <- HashMap.lookup "num_object_copies" statsSum
  , Just (Aeson.Number missing)     <- HashMap.lookup "num_objects_missing_on_primary" statsSum
  , Just (Aeson.Number degraded)    <- HashMap.lookup "num_objects_degraded" statsSum
  , Just (Aeson.Number unfound)     <- HashMap.lookup "num_objects_unfound" statsSum
  , Just (Aeson.Number recovered)   <- HashMap.lookup "num_objects_recovered" statsSum

  , Just (Aeson.Object osdStatsSum) <- HashMap.lookup "osd_stats_sum" obj
  , Just (Aeson.Number kbUsed)      <- HashMap.lookup "kb_used" osdStatsSum
  , Just (Aeson.Number kbTotal)     <- HashMap.lookup "kb" osdStatsSum

  = let et = def & Riemann.host .~ Just "ceph"
                 & Riemann.time .~ Just (floor $ utcTimeToPOSIXSeconds ts')

        realToFloat :: Real a => a -> Maybe Double
        realToFloat = Just . realToFrac

    in [ et & Riemann.service .~ Just "object clones"    & Riemann.metric .~ realToFloat (clones/objects)
       , et & Riemann.service .~ Just "copies"           & Riemann.metric .~ realToFloat (copies/objects)
       , et & Riemann.service .~ Just "object missing"   & Riemann.metric .~ realToFloat (missing/objects)
       , et & Riemann.service .~ Just "object degraded"  & Riemann.metric .~ realToFloat (degraded/objects)
       , et & Riemann.service .~ Just "object unfound"   & Riemann.metric .~ realToFloat (unfound/objects)
       , et & Riemann.service .~ Just "object recovered" & Riemann.metric .~ realToFloat (recovered/objects)
       , et & Riemann.service .~ Just "fs used"          & Riemann.metric .~ realToFloat (kbUsed/kbTotal)
       ]

  | otherwise = []

valueToMessage :: (a, Aeson.Value) -> Riemann.Msg
valueToMessage (_, Aeson.Object obj)
  | Just (Aeson.String _ver)            <- HashMap.lookup "version" obj
  , Just (Aeson.Object healthObj)       <- HashMap.lookup "health" obj
  , Just (Aeson.Array healthSummaryArr) <- HashMap.lookup "summary" healthObj
  , Just (Aeson.String healthStatus)    <- HashMap.lookup "overall_status" healthObj
  , [Aeson.Object healthSummaryObj]     <- Vector.toList healthSummaryArr
  , Just (Aeson.String healthSummary)   <- HashMap.lookup "summary" healthSummaryObj
  , Just (Aeson.String ts)              <- HashMap.lookup "timestamp" obj
  , Just ts'                            <- parseJSONTime ts
  , Just (Aeson.Object pgmap)           <- HashMap.lookup "pgmap" obj

  = let evt = def
          & Riemann.time         .~ Just (floor $ utcTimeToPOSIXSeconds ts')
          & Riemann.state        .~ Just healthStatus
          & Riemann.service      .~ Just "health"
          & Riemann.host         .~ Just "ceph"
          & Riemann.description  .~ Just healthSummary

    in def & Riemann.events .~ (evt:pgmapEvents pgmap)

valueToMessage _ = error "Incomprehensible failure"

-- Run 'ceph report' and parse the output into a stream of messages
cephMon :: MonadResource m => Source m Riemann.Msg
cephMon =
  sourceProcess (proc "ceph" ["report"])
    =$= conduitParser cephJson
    =$= Cl.map valueToMessage

-- The output of 'ceph report' is wrapped with some extra crap that we don't
-- normally care about, we'll just wrap Aeson's parser and discard the excess.
--
-- -------- BEGIN REPORT --------
-- {json}
-- -------- END REPORT 3530661700 --------
--
cephJson :: Atto.Parser Aeson.Value
cephJson =
  let eol = Atto.skipWhile (Prelude.not . Atto8.isEndOfLine) *> Atto8.endOfLine
      beg = Atto8.skipSpace *> Atto.string "-------- BEGIN REPORT" *> eol
      end = Atto8.skipSpace *> Atto.string "-------- END REPORT"   *> eol
  in beg *> Aeson.json' <* end
