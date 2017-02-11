{-# LANGUAGE RecursiveDo #-}
{-# OPTIONS_GHC -fno-warn-deprecations #-}

-- | In-memory implementation of the Transport API.
module Network.Transport.InMemory
  ( createTransport
  , createTransportExposeInternals
  -- * For testing purposes
  , TransportInternals(..)
  , TransportState(..)
  , ValidTransportState(..)
  , breakConnection
  ) where

import Control.Monad (forM)
import Control.Applicative
import Network.Transport
import Network.Transport.Internal ( mapIOException )
import Control.Category ((>>>))
import Control.Concurrent.STM
import Control.Exception (handle, throw)
import Data.Word (Word32, Word64)
import Data.Bits (shiftL, shiftR, (.|.))
import Data.Map (Map)
import Data.Maybe (fromJust)
import Data.Monoid
import Data.Foldable
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC (pack)
import Data.Accessor (Accessor, accessor, (^.), (^=), (^:))
import qualified Data.Accessor.Container as DAC (mapMaybe)
import Data.Typeable (Typeable)
import Prelude hiding (foldr)

data TransportState
  = TransportValid {-# UNPACK #-} !ValidTransportState
  | TransportClosed

data ValidTransportState = ValidTransportState
  { _localEndPoints :: !(Map EndPointAddress LocalEndPoint)
  , _nextLocalEndPointId :: !Int
  }

data LocalEndPoint = LocalEndPoint
  { localEndPointAddress :: !EndPointAddress
  , localEndPointChannel :: !(TChan Event)
  , localEndPointState   :: !(TVar LocalEndPointState)
  }

data LocalEndPointState
  = LocalEndPointValid {-# UNPACK #-} !ValidLocalEndPointState
  | LocalEndPointClosed

data ValidLocalEndPointState = ValidLocalEndPointState
  { _nextHeavyweightConnectionId :: !HeavyweightConnectionId
  , _nextLightweightConnectionId :: !LightweightConnectionId
    -- For each EndPointAddress (a peer) there can be only one
    -- HeavyweightConnectionId at any given time.
  , _connections :: !(Map EndPointAddress LocalConnections)
  , _multigroups :: Map MulticastAddress (TVar (Set EndPointAddress))
  }

-- | Connections to another EndPoint. Connector chooses the
--   HeavyweightConnectionId, connected chooses the LightweightConnectionId.
data LocalConnections = LocalConnections
  { localConnectionsHeavyweightConnectionId :: !HeavyweightConnectionId
  , localConnections :: !(Map LightweightConnectionId LocalConnection)
  }

data LocalConnection = LocalConnection
  { localConnectionId :: !ConnectionId
  , localConnectionLocalAddress :: !EndPointAddress
  , localConnectionRemoteAddress :: !EndPointAddress
  , localConnectionState :: !(TVar LocalConnectionState)
  }

data LocalConnectionState
  = LocalConnectionValid
  | LocalConnectionClosed
  | LocalConnectionFailed

-- | A 'ConnectionId' is the concatenation of a
--   'HeavyweightConnectionId' and a 'LightweightConnectionId'.
--   See 'createConnectionId'.
type LightweightConnectionId = Word32

firstLightweightConnectionId :: LightweightConnectionId
firstLightweightConnectionId = 0

-- | A 'ConnectionId' is the concatenation of a
--   'HeavyweightConnectionId' and a 'LightweightConnectionId'.
--   See 'createConnectionId'.
type HeavyweightConnectionId = Word32

-- [Note: computing the ConnectionId]
--
-- Suppose A connects to B. From these two EndPoints we must come up with
-- a ConnectionId which is unique in B, and also a HeavyweightConnectionId
-- which is unique in A (so that it can be given to the Connection constructor).
-- The solution: source the LightweightConnectionId from B (it knows the next
-- one to use) and the HeavyweightConnectionId from A (similarly, it knows the
-- next one to use).

firstHeavyweightConnectionId :: HeavyweightConnectionId
firstHeavyweightConnectionId = 0

-- | Use a 'LightweightConnectionId' and a 'HeavyweightConnectionId' to build
--   a network-transport 'ConnectionId'.
createConnectionId
  :: HeavyweightConnectionId
  -> LightweightConnectionId
  -> ConnectionId
createConnectionId hcid lcid =
  (makeWord64 hcid `shiftL` 32) .|. makeWord64 lcid
  where
  makeWord64 :: Word32 -> Word64
  makeWord64 = fromIntegral

-- | Decompose a 64-bit `ConnectionId` into its 32-bit heavyweight and
--   lightweight identifier components.
--
--     uncurry createConnectionId . deconstructConnectionId = id
--
deconstructConnectionId
  :: ConnectionId
  -> (HeavyweightConnectionId, LightweightConnectionId)
deconstructConnectionId connid =
  (makeWord32 (connid `shiftR` 32), makeWord32 connid)
  where
  makeWord32 :: Word64 -> Word32
  makeWord32 = fromIntegral

newtype TransportInternals = TransportInternals (TVar TransportState)

-- | Create a new Transport.
--
-- Only a single transport should be created per Haskell process
-- (threads can, and should, create their own endpoints though).
createTransport :: IO Transport
createTransport = fst <$> createTransportExposeInternals

-- | Create a new Transport exposing internal state.
--
-- Useful for testing and/or debugging purposes.
-- Should not be used in production. No guarantee as to the stability of the internals API.
createTransportExposeInternals :: IO (Transport, TransportInternals)
createTransportExposeInternals = do
  state <- newTVarIO $ TransportValid $ ValidTransportState
    { _localEndPoints = Map.empty
    , _nextLocalEndPointId = 0
    }
  return (Transport
    { newEndPoint    = apiNewEndPoint state
    , closeTransport = do
        -- transactions are splitted into smaller ones intentionally
        old <- atomically $ swapTVar state TransportClosed
        case old of
          TransportClosed -> return ()
          TransportValid tvst -> do
            forM_ (tvst ^. localEndPoints) $ \l -> do
              cons <- atomically $ whenValidLocalEndPointState l $ \lvst -> do
                writeTChan (localEndPointChannel l) EndPointClosed
                writeTVar  (localEndPointState l) LocalEndPointClosed
                return $ do
                  (_, lconns) <- Map.toList (lvst ^. connections)
                  (_, lconn) <- Map.toList (localConnections lconns)
                  return lconn
              forM_ cons $ \con -> atomically $
                writeTVar (localConnectionState con) LocalConnectionClosed
    }, TransportInternals state)

-- | Create a new end point.
apiNewEndPoint :: TVar TransportState
               -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint state = handle (return . Left) $ atomically $ do
  chan <- newTChan
  (lep,addr) <- withValidTransportState state NewEndPointFailed $ \vst -> do
    lepState <- newTVar $ LocalEndPointValid $ ValidLocalEndPointState
      { _nextHeavyweightConnectionId = firstHeavyweightConnectionId
      , _nextLightweightConnectionId = firstLightweightConnectionId
      , _connections = Map.empty
      , _multigroups = Map.empty
      }
    let r = nextLocalEndPointId ^: (+ 1) $ vst
        addr = EndPointAddress . BSC.pack . show $ r ^. nextLocalEndPointId
        lep = LocalEndPoint
          { localEndPointAddress = addr
          , localEndPointChannel = chan
          , localEndPointState = lepState
          }
    writeTVar state (TransportValid $ localEndPointAt addr ^= Just lep $ r)
    return (lep, addr)
  return $ Right $ EndPoint
    { receive       = atomically $ do
        result <- tryReadTChan chan
        case result of
          Nothing -> do st <- readTVar (localEndPointState lep)
                        case st of
                          LocalEndPointClosed ->
                            throwSTM (userError "Channel is closed.")
                          LocalEndPointValid{} -> retry
          Just x -> return x
    , address       = addr
    , connect       = apiConnect addr state
    , closeEndPoint = apiCloseEndPoint state addr
    , newMulticastGroup     = return $ Left $ newMulticastGroupError
    , resolveMulticastGroup = return . Left . const resolveMulticastGroupError
    }
  where
    -- see [Multicast] section
    newMulticastGroupError =
      TransportError NewMulticastGroupUnsupported "Multicast not supported"
    resolveMulticastGroupError =
      TransportError ResolveMulticastGroupUnsupported "Multicast not supported"

apiCloseEndPoint :: TVar TransportState -> EndPointAddress -> IO ()
apiCloseEndPoint state addr = atomically $ whenValidTransportState state $ \vst ->
    forM_ (vst ^. localEndPointAt addr) $ \lep -> do
      old <- swapTVar (localEndPointState lep) LocalEndPointClosed
      case old of
        LocalEndPointClosed -> return ()
        LocalEndPointValid lepvst -> do
          let lst = do
                (addr, lconns) <- Map.toList (lepvst ^. connections)
                (lwcid, lconn) <- Map.toList (localConnections lconns)
                return lconn
          forM_ lst $ \lconn -> do
            st <- swapTVar (localConnectionState lconn) LocalConnectionClosed
            case st of
              LocalConnectionClosed -> return ()
              LocalConnectionFailed -> return ()
              _ -> forM_ (vst ^. localEndPointAt (localConnectionRemoteAddress lconn)) $ \thep ->
                     whenValidLocalEndPointState thep $ \_ -> do
                        writeTChan (localEndPointChannel thep)
                                   (ConnectionClosed (localConnectionId lconn))
          writeTChan (localEndPointChannel lep) EndPointClosed
          writeTVar  (localEndPointState lep)   LocalEndPointClosed
      writeTVar state (TransportValid $ (localEndPoints ^: Map.delete addr) vst)

-- | Function that simulate failing connection between two endpoints,
-- after calling this function both endpoints will receive ConnectionEventLost
-- message, and all @LocalConnectionValid@ connections will
-- be put into @LocalConnectionFailed@ state.
breakConnection :: TransportInternals
                -> EndPointAddress
                -> EndPointAddress
                -> String                   -- ^ Error message
                -> IO ()
breakConnection (TransportInternals state) from to message =
  atomically $ apiBreakConnection state from to message


-- | Tear down functions that should be called in case if conncetion fails.
apiBreakConnection :: TVar TransportState
                   -> EndPointAddress
                   -> EndPointAddress
                   -> String
                   -> STM ()
apiBreakConnection state us them msg
  | us == them = return ()
  | otherwise  = whenValidTransportState state $ \vst -> do
      breakOne vst us them >> breakOne vst them us
  where
    breakOne vst a b = do
      forM_ (vst ^. localEndPointAt a) $ \lep ->
        whenValidLocalEndPointState lep $ \lepvst -> do
          let mlconns = Map.lookup b (lepvst ^. connections)
          case mlconns of
            Nothing -> return ()
            Just lconns -> do
              let hwcid = localConnectionsHeavyweightConnectionId lconns
              let connList = Map.toList (localConnections lconns)
              lcids <- forM connList $ \(lcid, conn) -> do
                modifyTVar (localConnectionState conn)
                           (\x -> case x of
                                    LocalConnectionValid -> LocalConnectionFailed
                                    _ -> x)
                return lcid
              writeTChan (localEndPointChannel lep)
                         (ErrorEvent (TransportError (EventConnectionLost b hwcid) msg))
              let connections' = Map.delete b (lepvst ^. connections)
              writeTVar (localEndPointState lep)
                        (LocalEndPointValid $ (connections ^= connections') lepvst)

-- | Create a new connection
apiConnect :: EndPointAddress
           -> TVar TransportState
           -> EndPointAddress
           -> Reliability
           -> ConnectHints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect ourAddress state theirAddress _reliability _hints = do
    handle (return . Left) $ fmap Right $ atomically $ do
      (chan, lconn, hwcid, lwcid) <- do
        withValidTransportState state ConnectFailed $ \vst -> do
          -- First, look up the local and remote 'EndPoint's in the transport
          -- state, failing if either is not there.
          ourlep <- case vst ^. localEndPointAt ourAddress of
                      Nothing ->
                        throwSTM $ TransportError ConnectFailed "Endpoint closed"
                      Just x  -> return x
          theirlep <- case vst ^. localEndPointAt theirAddress of
                        Nothing ->
                          throwSTM $ TransportError ConnectNotFound "Endpoint not found"
                        Just x  -> return x
          -- Increment and return the LightweightConnectionId for the remote
          -- one.
          lwcid <- withValidLocalEndPointState theirlep ConnectFailed $ \lepvst -> do
            let r = nextLightweightConnectionId ^: (+ 1) $ lepvst
            writeTVar (localEndPointState theirlep) (LocalEndPointValid r)
            return (r ^. nextLightweightConnectionId)
          -- There may already be a connection. In that case, re-use the
          -- 'HeavyweightConnectionId'. Otherwise, use the next one.
          withValidLocalEndPointState ourlep ConnectFailed $ \lepvst -> do
            (hwcid, connections') <- case Map.lookup theirAddress (lepvst ^. connections) of
              -- This is the first connection to that peer.
              Nothing -> do
                let r = nextHeavyweightConnectionId ^: (+ 1) $ lepvst
                writeTVar (localEndPointState ourlep) (LocalEndPointValid r)
                let hwcid = r ^. nextHeavyweightConnectionId
                let connections' = \lconn -> Map.insert theirAddress (LocalConnections hwcid (Map.singleton lwcid lconn)) (lepvst ^. connections)
                return (hwcid, connections')
              -- We already have a connection to this peer.
              Just lconns -> do
                let hwcid = localConnectionsHeavyweightConnectionId lconns
                let adjuster lconn lconns = lconns {
                        localConnections = Map.insert lwcid lconn (localConnections lconns)
                      }
                let connections' = \lconn -> Map.adjust (adjuster lconn) theirAddress (lepvst ^. connections)
                return (hwcid, connections')
            let connid = createConnectionId hwcid lwcid
            lconnState <- newTVar LocalConnectionValid
            let lconn = LocalConnection
                           { localConnectionId = connid
                           , localConnectionLocalAddress = ourAddress
                           , localConnectionRemoteAddress = theirAddress
                           , localConnectionState = lconnState
                           }
            writeTVar (localEndPointState ourlep)
                      (LocalEndPointValid $ connections ^= (connections' lconn) $ lepvst)
            return (localEndPointChannel theirlep, lconn, hwcid, lwcid)
      writeTChan chan $
        ConnectionOpened (localConnectionId lconn) ReliableOrdered ourAddress
      return $ Connection
        { send  = apiSend chan state lconn
        , close = apiClose chan state lconn
        , bundle = hwcid
        }

-- | Send a message over a connection
apiSend :: TChan Event
        -> TVar TransportState
        -> LocalConnection
        -> [ByteString]
        -> IO (Either (TransportError SendErrorCode) ())
apiSend chan state lconn msg = handle handleFailure $ mapIOException sendFailed $
    atomically $ do
      connst <- readTVar (localConnectionState lconn)
      case connst of
        LocalConnectionValid -> do
          foldr seq () msg `seq`
            writeTChan chan (Received (localConnectionId lconn) msg)
          return $ Right ()
        LocalConnectionClosed -> do
          -- If the local connection was closed, check why.
          withValidTransportState state SendFailed $ \vst -> do
            let addr = localConnectionLocalAddress lconn
                mblep = vst ^. localEndPointAt addr
            case mblep of
              Nothing -> throwSTM $ TransportError SendFailed "Endpoint closed"
              Just lep -> do
                lepst <- readTVar (localEndPointState lep)
                case lepst of
                  LocalEndPointValid _ -> do
                    return $ Left $ TransportError SendClosed "Connection closed"
                  LocalEndPointClosed -> do
                    throwSTM $ TransportError SendFailed "Endpoint closed"
        LocalConnectionFailed -> return $
          Left $ TransportError SendFailed "Endpoint closed"
    where
      sendFailed = TransportError SendFailed . show
      handleFailure ex@(TransportError SendFailed reason) = atomically $ do
        apiBreakConnection state (localConnectionLocalAddress lconn)
                                 (localConnectionRemoteAddress lconn)
                                 reason
        return (Left ex)
      handleFailure ex = return (Left ex)

-- | Close a connection
apiClose :: TChan Event
         -> TVar TransportState
         -> LocalConnection
         -> IO ()
apiClose chan state lconn = do
  atomically $ do -- XXX: whenValidConnectionState
    connst <- readTVar (localConnectionState lconn)
    case connst of
      LocalConnectionValid -> do
        writeTChan chan $ ConnectionClosed (localConnectionId lconn)
        writeTVar (localConnectionState lconn) LocalConnectionClosed
        whenValidTransportState state $ \vst -> do
          let mblep = vst ^. localEndPointAt (localConnectionLocalAddress lconn)
              theirAddress = localConnectionRemoteAddress lconn
          let updater :: LocalConnections -> Maybe LocalConnections
              updater lconns =
                let localConnections' = Map.delete lcid (localConnections lconns)
                in  if Map.null localConnections'
                    then Nothing
                    else Just $ lconns {
                             localConnections = localConnections'
                           }
          forM_ mblep $ \lep ->
            whenValidLocalEndPointState lep $
              writeTVar (localEndPointState lep)
                . LocalEndPointValid
                . (connections ^: Map.update updater theirAddress)
      _ -> return ()
  where
  (_, lcid) = deconstructConnectionId (localConnectionId lconn)

-- [Multicast]
-- Currently multicast implementation doesn't pass it's tests, so it
-- disabled. Here we have old code that could be improved, see GitHub ISSUE 5
-- https://github.com/haskell-distributed/network-transport-inmemory/issues/5

-- | Create a new multicast group
_apiNewMulticastGroup :: TVar TransportState
                     -> EndPointAddress
                     -> IO (Either (TransportError NewMulticastGroupErrorCode) MulticastGroup)
_apiNewMulticastGroup state ourAddress = handle (return . Left) $ do
  group <- newTVarIO Set.empty
  groupAddr <- atomically $
    withValidTransportState state NewMulticastGroupFailed $ \vst -> do
      lep <- maybe (throwSTM $ TransportError NewMulticastGroupFailed "Endpoint closed")
                   return
                   (vst ^. localEndPointAt ourAddress)
      withValidLocalEndPointState lep NewMulticastGroupFailed $ \lepvst -> do
        let addr = MulticastAddress . BSC.pack . show . Map.size $ lepvst ^. multigroups
        writeTVar (localEndPointState lep) (LocalEndPointValid $ multigroupAt addr ^= group $ lepvst)
        return addr
  return . Right $ createMulticastGroup state ourAddress groupAddr group

-- | Construct a multicast group
--
-- When the group is deleted some endpoints may still receive messages, but
-- subsequent calls to resolveMulticastGroup will fail. This mimicks the fact
-- that some multicast messages may still be in transit when the group is
-- deleted.
createMulticastGroup :: TVar TransportState
                     -> EndPointAddress
                     -> MulticastAddress
                     -> TVar (Set EndPointAddress)
                     -> MulticastGroup
createMulticastGroup state ourAddress groupAddress group = MulticastGroup
    { multicastAddress     = groupAddress
    , deleteMulticastGroup = atomically $
        whenValidTransportState state $ \vst -> do
          -- XXX best we can do given current broken API, which needs fixing.
          let lep = fromJust $ vst ^. localEndPointAt ourAddress
          modifyTVar' (localEndPointState lep) $ \lepst -> case lepst of
            LocalEndPointValid lepvst ->
              LocalEndPointValid $ multigroups ^: Map.delete groupAddress $ lepvst
            LocalEndPointClosed ->
              LocalEndPointClosed
    , maxMsgSize           = Nothing
    , multicastSend        = \payload -> atomically $
        withValidTransportState state SendFailed $ \vst -> do
          es <- readTVar group
          forM_ (Set.elems es) $ \ep -> do
            let ch = localEndPointChannel $ fromJust $ vst ^. localEndPointAt ep
            writeTChan ch (ReceivedMulticast groupAddress payload)
    , multicastSubscribe   = atomically $ modifyTVar' group $ Set.insert ourAddress
    , multicastUnsubscribe = atomically $ modifyTVar' group $ Set.delete ourAddress
    , multicastClose       = return ()
    }

-- | Resolve a multicast group
_apiResolveMulticastGroup :: TVar TransportState
                         -> EndPointAddress
                         -> MulticastAddress
                         -> IO (Either (TransportError ResolveMulticastGroupErrorCode) MulticastGroup)
_apiResolveMulticastGroup state ourAddress groupAddress = handle (return . Left) $ atomically $
    withValidTransportState state ResolveMulticastGroupFailed $ \vst -> do
      lep <- maybe (throwSTM $ TransportError ResolveMulticastGroupFailed "Endpoint closed")
                   return
                   (vst ^. localEndPointAt ourAddress)
      withValidLocalEndPointState lep ResolveMulticastGroupFailed $ \lepvst -> do
          let group = lepvst ^. (multigroups >>> DAC.mapMaybe groupAddress)
          case group of
            Nothing ->
              return . Left $
                TransportError ResolveMulticastGroupNotFound
                  ("Group " ++ show groupAddress ++ " not found")
            Just mvar ->
              return . Right $ createMulticastGroup state ourAddress groupAddress mvar

--------------------------------------------------------------------------------
-- Lens definitions                                                           --
--------------------------------------------------------------------------------

nextLocalEndPointId :: Accessor ValidTransportState Int
nextLocalEndPointId = accessor _nextLocalEndPointId (\eid st -> st{ _nextLocalEndPointId = eid} )

localEndPoints :: Accessor ValidTransportState (Map EndPointAddress LocalEndPoint)
localEndPoints = accessor _localEndPoints (\leps st -> st { _localEndPoints = leps })

nextLightweightConnectionId :: Accessor ValidLocalEndPointState LightweightConnectionId
nextLightweightConnectionId = accessor _nextLightweightConnectionId (\lwcid st -> st { _nextLightweightConnectionId = lwcid })

nextHeavyweightConnectionId :: Accessor ValidLocalEndPointState HeavyweightConnectionId
nextHeavyweightConnectionId = accessor _nextHeavyweightConnectionId (\hwcid st -> st { _nextHeavyweightConnectionId = hwcid })

connections :: Accessor ValidLocalEndPointState (Map EndPointAddress LocalConnections)
connections = accessor _connections (\conns st -> st { _connections = conns })

multigroups :: Accessor ValidLocalEndPointState (Map MulticastAddress (TVar (Set EndPointAddress)))
multigroups = accessor _multigroups (\gs st -> st { _multigroups = gs })

at :: Ord k => k -> String -> Accessor (Map k v) v
at k err = accessor (Map.findWithDefault (error err) k) (Map.insert k)

localEndPointAt :: EndPointAddress -> Accessor ValidTransportState (Maybe LocalEndPoint)
localEndPointAt addr = localEndPoints >>> DAC.mapMaybe addr

multigroupAt :: MulticastAddress -> Accessor ValidLocalEndPointState (TVar (Set EndPointAddress))
multigroupAt addr = multigroups >>> at addr "Invalid multigroup"

---------------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------------

-- | LocalEndPoint state deconstructor.
overValidLocalEndPointState :: LocalEndPoint -> STM a -> (ValidLocalEndPointState -> STM a) -> STM a
overValidLocalEndPointState lep fallback action = do
  lepst <- readTVar (localEndPointState lep)
  case lepst of
    LocalEndPointValid lepvst -> action lepvst
    _ -> fallback

-- | Specialized deconstructor that throws TransportError in case of Closed state
withValidLocalEndPointState :: (Typeable e, Show e) => LocalEndPoint -> e -> (ValidLocalEndPointState -> STM a) -> STM a
withValidLocalEndPointState lep ex = overValidLocalEndPointState lep (throw $ TransportError ex "EndPoint closed")

-- | Specialized deconstructor that do nothing in case of failure
whenValidLocalEndPointState :: Monoid m => LocalEndPoint -> (ValidLocalEndPointState -> STM m) -> STM m
whenValidLocalEndPointState lep = overValidLocalEndPointState lep (return mempty)

overValidTransportState :: TVar TransportState -> STM a -> (ValidTransportState -> STM a) -> STM a
overValidTransportState ts fallback action = do
  tsst <- readTVar ts
  case  tsst of
    TransportValid tsvst -> action tsvst
    _ -> fallback

withValidTransportState :: (Typeable e, Show e) => TVar TransportState -> e -> (ValidTransportState -> STM a) -> STM a
withValidTransportState ts ex = overValidTransportState ts (throw $ TransportError ex "Transport closed")

whenValidTransportState :: Monoid m => TVar TransportState -> (ValidTransportState -> STM m) -> STM m
whenValidTransportState ts = overValidTransportState ts (return mempty)
