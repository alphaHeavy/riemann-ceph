{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Monad (forever, void)
import Control.Monad.Trans (liftIO)
import qualified Data.Attoparsec.ByteString as Atto
import qualified Data.Attoparsec.ByteString.Char8 as Atto8
import qualified Data.Aeson as Aeson
import Data.Conduit
import Data.Conduit.Internal (conduitToPipe)
import Data.Conduit.Attoparsec (conduitParser)
import qualified Data.Conduit.List as Cl
import Data.Conduit.Process (proc, sourceProcess)
import qualified Data.Conduit.Network.UDP as UDP
import Data.Int
import Data.Monoid
import Data.ProtocolBuffers
import Data.Serialize (runPut)
import Data.TypeLevel hiding (Bool)
import Data.Text (Text)
import qualified Data.Text as T
import GHC.Generics (Generic)
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Vector as Vector
import Data.Time
import Data.Time.Clock.POSIX
import System.Locale (defaultTimeLocale)
import Network.Socket

{-
failMaybe :: a -> Maybe b -> Either a b
failMaybe _ (Just v) = Right v
failMaybe x _        = Left x

foo :: Aeson.Value -> Either String RiemannMsg
foo (Aeson.Object obj) = do
  ver <- failMaybe "version"   $ HashMap.lookup "version" obj
  Aeson.String ts  <- failMaybe "timestamp" $ HashMap.lookup "timestamp" obj
  Aeson.Object healthObj       <- failMaybe "health" $ HashMap.lookup "health" obj
  Aeson.Array healthSummaryArr <- failMaybe "summary" $ HashMap.lookup "summary" healthObj
  Aeson.String healthStatus    <- failMaybe "overall_status" $ HashMap.lookup "overall_status" healthObj


  case Vector.toList healthSummaryArr of
    [Aeson.Object healthSummaryObj] -> do
      Aeson.String healthSummary <- failMaybe "overall_status/summary" $ HashMap.lookup "summary" healthSummaryObj
      return undefined
-}

parseJSONTime :: Text -> Maybe UTCTime
parseJSONTime = parseTime defaultTimeLocale "%Y-%m-%d %H:%M:%S%Q" . T.unpack

pgmapEvents :: Aeson.Object -> [RiemannEvent]
pgmapEvents obj
  | Just (Aeson.String ts) <- HashMap.lookup "stamp" obj
  , Just ts' <- parseJSONTime ts
  , Just (Aeson.Object pgStatsSum) <- HashMap.lookup "pg_stats_sum" obj
  , Just (Aeson.Object statsSum)   <- HashMap.lookup "stat_sum" pgStatsSum
  , Just (Aeson.Number objects)    <- HashMap.lookup "num_objects" statsSum
  , Just (Aeson.Number clones)     <- HashMap.lookup "num_object_clones" statsSum
  , Just (Aeson.Number copies)     <- HashMap.lookup "num_object_copies" statsSum
  , Just (Aeson.Number missing)    <- HashMap.lookup "num_objects_missing_on_primary" statsSum
  , Just (Aeson.Number degraded)   <- HashMap.lookup "num_objects_degraded" statsSum
  , Just (Aeson.Number unfound)    <- HashMap.lookup "num_objects_unfound" statsSum
  , Just (Aeson.Number recovered)  <- HashMap.lookup "num_objects_recovered" statsSum

  , Just (Aeson.Object osdStatsSum) <- HashMap.lookup "osd_stats_sum" obj
  , Just (Aeson.Number kbUsed)      <- HashMap.lookup "kb_used" osdStatsSum
  , Just (Aeson.Number kbTotal)     <- HashMap.lookup "kb" osdStatsSum

  = let et = RiemannEvent
          { eventTime         = putField . Just . floor $ utcTimeToPOSIXSeconds ts'
          , eventState        = mempty
          , eventService      = mempty
          , eventHost         = putField $ Just "lv1srv002"
          , eventDescription  = mempty
          , eventTags         = mempty
          , eventTtl          = mempty
          , eventAttributes   = mempty
          , eventMetricSInt64 = mempty
          , eventMetricD      = mempty
          , eventMetricF      = mempty
          }

    in [ et{eventService = putField $ Just "ceph /pgmap/pg_sum/ceph clones", eventMetricF = putField . Just . realToFrac $ clones/objects}
       , et{eventService = putField $ Just "ceph /pgmap/pg_sum/copies", eventMetricF = putField . Just . realToFrac $ copies/objects}
       , et{eventService = putField $ Just "ceph /pgmap/pg_sum/missing", eventMetricF = putField . Just . realToFrac $ missing/objects}
       , et{eventService = putField $ Just "ceph /pgmap/pg_sum/degraded", eventMetricF = putField . Just . realToFrac $ degraded/objects}
       , et{eventService = putField $ Just "ceph /pgmap/pg_sum/unfound", eventMetricF = putField . Just . realToFrac $ unfound/objects}
       , et{eventService = putField $ Just "ceph /pgmap/pg_sum/recovered", eventMetricF = putField . Just . realToFrac $ recovered/objects}
       , et{eventService = putField $ Just "ceph /pgmap/osd_sum/fsused", eventMetricF = putField . Just . realToFrac $ kbUsed/kbTotal}
       ]

  | otherwise = []

valueToMessage :: (a, Aeson.Value) -> RiemannMsg
valueToMessage (_, Aeson.Object obj)
  | Just (Aeson.String ver) <- HashMap.lookup "version" obj
  , Just (Aeson.Object healthObj) <- HashMap.lookup "health" obj
  , Just (Aeson.Array healthSummaryArr) <- HashMap.lookup "summary" healthObj
  , Just (Aeson.String healthStatus) <- HashMap.lookup "overall_status" healthObj
  , [Aeson.Object healthSummaryObj] <- Vector.toList healthSummaryArr
  , Just (Aeson.String healthSummary) <- HashMap.lookup "summary" healthSummaryObj
  -- "timestamp": "2013-03-09 22:47:03.171579",
  , Just (Aeson.String ts) <- HashMap.lookup "timestamp" obj
  , Just ts' <- parseJSONTime ts
  , Just (Aeson.Object pgmap) <- HashMap.lookup "pgmap" obj

  = let healthy = case healthStatus of
          "HEALTH_OK"   -> Just True
          "HEALTH_WARN" -> Just False
          "HEALTH_ERR"  -> Just False
          _             -> Nothing

        evt = RiemannEvent
          { eventTime         = putField . Just . floor $ utcTimeToPOSIXSeconds ts'
          , eventState        = putField $ Just healthStatus
          , eventService      = putField $ Just "ceph"
          , eventHost         = putField $ Just "lv1srv002"
          , eventDescription  = putField $ Just healthSummary
          , eventTags         = mempty
          , eventTtl          = mempty
          , eventAttributes   = mempty
          , eventMetricSInt64 = mempty
          , eventMetricD      = mempty
          , eventMetricF      = mempty
          }

    in RiemannMsg
         { msgOk     = putField healthy
         , msgError  = mempty
         , msgStates = mempty
         , msgQuery  = mempty
         , msgEvents = putField (evt:pgmapEvents pgmap)
         }

  | otherwise = undefined

cephMon :: MonadResource m => GSource m RiemannMsg
cephMon =
  sourceProcess (proc "ceph" ["report"])
    >+> conduitToPipe (conduitParser cephJson)
    >+> Cl.map valueToMessage

cephJson :: Atto.Parser Aeson.Value
cephJson =
  let eol = Atto.skipWhile (Prelude.not . Atto8.isEndOfLine) *> Atto8.endOfLine
      beg = Atto8.skipSpace *> Atto.string "-------- BEGIN REPORT" *> eol
      end = Atto8.skipSpace *> Atto.string "-------- END REPORT"   *> eol
  in beg *> Aeson.json' <* end

main :: IO ()
main = do
  (s, AddrInfo{addrAddress = addr}) <- UDP.getSocket "127.0.0.1" 5555
  void . runResourceT . runPipe $ forever (cephMon >> liftIO (threadDelay 10000000))
    >+> Cl.map (runPut . encodeMessage)
    >+> Cl.map (`UDP.Message` addr)
    >+> UDP.sinkToSocket s

data RiemannState = RiemannState
  { stateTime        :: Optional D1 (Value Int64)
  , stateState       :: Optional D2 (Value Text)
  , stateService     :: Optional D3 (Value Text)
  , stateHost        :: Optional D4 (Value Text)
  , stateDescription :: Optional D5 (Value Text)
  , stateOnce        :: Optional D6 (Value Bool)
  , stateTags        :: Repeated D7 (Value Text)
  , stateTtl         :: Optional D8 (Value Float)
  } deriving (Generic, Show)

instance Encode RiemannState

data RiemannEvent = RiemannEvent
  { eventTime         :: Optional D1 (Value Int64)
  , eventState        :: Optional D2 (Value Text)
  , eventService      :: Optional D3 (Value Text)
  , eventHost         :: Optional D4 (Value Text)
  , eventDescription  :: Optional D5 (Value Text)
  , eventTags         :: Repeated D7 (Value Text)
  , eventTtl          :: Optional D7 (Value Float)
  , eventAttributes   :: Repeated D7 (Message RiemannAttribute)
  , eventMetricSInt64 :: Optional D13 (Value (Signed Int64))
  , eventMetricD      :: Optional D14 (Value Double)
  , eventMetricF      :: Optional D15 (Value Float)
  } deriving (Generic, Show)

instance Encode RiemannEvent

data RiemannQuery = RiemannQuery
  { queryString :: Optional D1 (Value Text)
  } deriving (Generic, Show)

instance Encode RiemannQuery

data RiemannMsg = RiemannMsg
  { msgOk     :: Optional D2 (Value Bool)
  , msgError  :: Optional D3 (Value Text)
  , msgStates :: Repeated D4 (Message RiemannState)
  , msgQuery  :: Optional D5 (Message RiemannQuery)
  , msgEvents :: Repeated D6 (Message RiemannEvent)
  } deriving (Generic, Show)

instance Encode RiemannMsg

data RiemannAttribute = RiemannAttribute
  { attributeKey   :: Required D1 (Value Text)
  , attributeValue :: Optional D2 (Value Text)
  } deriving (Generic, Show)

instance Encode RiemannAttribute
