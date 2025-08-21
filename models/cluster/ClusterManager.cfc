/**
 * I manage the cluster peers for the SocketBox cluster.
 * I am a singleton stored in the application scope.
 */
component accessors="true" {
	/**
	 * Struct of ClusterPeer instance represnting our cluster peers
	 * The struct key is the peer name, and the value is the ClusterPeer instance
	 */
	property name="peerConnections" type="struct";

	/**
	 * The SocketBox instance.  This isn't a singleton per se, but there's no need to re-create it
	 */
	property name="socketBox" type="any";

	/**
	 * Delay in seconds before checking peer connections again
	 * This will reset to a lower number any time there is a change, and slowly increase as the cluster stabilizes
	 */
	property name="delaySeconds" type="numeric";

	/**
	 * The tick count when the next run should occur
	 */
	property name="nextRunTick" type="numeric";

	/**
	 * The last time the peer connections were modified
	 */
	property name="lastUpdateTick" type="numeric";

	/**
	 * The lock name used for synchronizing access to the peer connections
	 * This is to accomodate more than one socketbox-enabled application in the same server
	 */
	property name="lockName" type="any";

	/**
	 * Cluster Manager Key
	 */
	property name="clusterManagerKey" type="string";

	/**
	 * Is there a cache provider configured?
	 */
	property name="cacheProviderExists" type="boolean";

	/**
	 * The configured cache provider
	 */
	property name="cacheProvider" type="any";

	/**
	 * The cache key used to store the list of cluster peers
	 */
	property name="cacheKeyPrefix" type="string";

	/**
	 * My peer name.  For convenience
	 */
	property name="myPeerName" type="string";

	/**
	 * Cluster RPC operations
	 */
	property name="RPCOperations" type="struct";

	/**
	 * Start tickount
	 */
	property name="startTick" type="numeric";

	/**
	 * Copy of config to prevent circular references during startup
	 */
	property name="config" type="struct";

	/**
	 * Constructor
	 * @socketBox The SocketBox instance to use for cluster management
	 * @config The configuration for the SocketBox instance.  This is mostly to avoid circular references. during startup.
	 * @return The ClusterManager instance
	 */
	function init( required any socketBox, required struct config ) {
		variables.startTick = getTickCount();
		variables.config = config;
		variables.clusterManagerKey = createUUID();
		variables.socketBox = socketBox;
		variables.peerConnections = {};
		variables.lastUpdateTick = getTickCount();
		variables.lockName = "socketbox-cluster-peer-lock-" & createUUID();
		variables.jThread = createObject( "java", "java.lang.Thread" );
		variables.jSystem = createObject( "java", "java.lang.System" );
		if( !isSimpleValue( config.cluster.cacheProvider ) ) {
			variables.cacheProviderExists = true;
			variables.cacheProvider = config.cluster.cacheProvider;
		} else {
			variables.cacheProviderExists = false;
		}
		variables.cacheKeyPrefix = "#config.cluster.cachePrefix#socketbox-cluster-peers";
		variables.myPeerName = config.cluster.name;
		RPCOperations={};
		recalcUpdateDelay();

		return this;
	}

	/**
	 * Start the cluster manager
	 * This will check the configured peers and ensure we have a connection to each of them.
	 * Then it will start the periodic check for peer connections.
	 */
	function start() {
		// Register ourselves as the current manager for this SocketBox instance
		server.socketBoxManagers[socketBox.getSocketBoxKey()] = variables.clusterManagerKey;

		// Immediate initial check
		checkPeers();

		// Now fire up management thread to keep in sync
		thread name="SocketBoxClusterManager" action="run" {
			cfsetting( requesttimeout=9999999999 );
			println( "SocketBox cluster manager thread started." );
			sleep( variables.delaySeconds * 1000 );

			// If the application has restarted and we are no longer the current manager, then this thread is done
			var updated = false;
			while( (server.socketBoxManagers[socketBox.getSocketBoxKey()] ?: '') == variables.clusterManagerKey ) {
				try {
					updated = false;
					updateCacheLastCheckin();
					if( nextRunTick <= getTickCount() ) {
						updated = true;
						//println("Checking SocketBox cluster peers after " & variables.delaySeconds & " seconds...");
						checkPeers();
					}
					// Check for peer connections every delaySeconds
					sleep( 2000 );
				} catch( any e ) {
					println("SocketBox error in SocketBox cluster manager: " & e.message);
				} finally {
					if( updated ) {
						recalcUpdateDelay();
					}
				}
			}
			println( "SocketBox cluster manager thread stopped." );
			shutdownPeerConnections();
		}
	}

	/**
	 * Mark the last checkin time in the cache provider
	 */
	function updateCacheLastCheckin() {
		if( !hasCacheProvider() ) {
			return;
		}
		var cacheKey = cacheKeyPrefix & "-" & myPeerName;
		variables.cacheProvider.set( cacheKey, getEpochSeconds() );
	}

	/**
	 * Get the current epoch seconds.  epoch should be in GMT, so the same regardless of timezone
	 * This is used to store the last checkin time in the cache provider
	 */
	function getEpochSeconds() {
		return int( jSystem.currentTimeMillis()/1000 );
	}

	/**
	 * Check for expired peers in the cache and remove them
	 */
	function reapExpiredCachePeers() {
		if( !hasCacheProvider() ) {
			return;
		}
		getCachePeers().each( (cachePeer)=>{
			if( isPeerExpired( cachePeer ) ) {
				// This peer has timed out
				println( "SocketBox Removing expired peer from cache: " & cachePeer );
				removedPeerFromCache( cachePeer );
			}
		}, true );
	}

	/**
	 * Check if the peer is expired based on the last checkin time
	 */
	function isPeerExpired( required string cachePeer ) {
		// Get this every time so it's fresh
		var nowEpochSeconds = getEpochSeconds();
		var lastCheckin = val( variables.cacheProvider.get( cacheKeyPrefix & "-" & cachePeer ) ?: '' );
		return lastCheckin < ( nowEpochSeconds - config.cluster.peerIdleTimeoutSeconds );
	}

	/**
	 * Get the manager node for this cluster
	 * The answer to this can change, so do not cache the output of this, at least not for more than a few seconds.
	 */
	function getManagerNode() {
		if( !hasCacheProvider() ) {
			if( variables.peerConnections.len() ) {
				return peerConnections.first();
			} else {
				return "";
			}
		}
		var cacheKey = cacheKeyPrefix & "-manager";
		// Who is the current manager?
		var manager = variables.cacheProvider.get( cacheKey ) ?: "";
		// If there is none set, or it's an invalid peer, then make ourselves the manager
		println( "found manager in cache: " & manager )
		println( "peerConnections.keyExists( manager ): " & peerConnections.keyExists( manager ) )
		println( "manager == getMyPeerName(): " & manager == getMyPeerName() )
		if( !manager.len() || !( variables.peerConnections.keyExists( manager ) || manager == getMyPeerName() ) ) {
			println( "SocketBox [#getMyPeerName()#] promoted to cluster manager." );
			variables.cacheProvider.set( cacheKey, getMyPeerName() );
			return getMyPeerName();
		}
		return manager;
	}

	/**
	 * Am I the captain now?
	 * The answer to this can change, so do not cache the output of this, at least not for more than a few seconds.
	 */
	function isManager() {
		return getManagerNode() == getMyPeerName();
	}

	/**
	 * I do a full review of all configured cluster peers and ensure there is a connection to each of them, 
	 * also removing any old connections.
	 */
	function checkPeers() {

		// make sure we're registered as a peer in the cache
		if( hasCacheProvider() ) {
			ensureMyselfInCache();

			reapExpiredCachePeers();
		}
		var currentPeers = getPeers();
		// Disconnect from any peers we no longer have configured
		variables.peerConnections.each( (peerName, peer)=>{
			if( !currentPeers.contains( peerName ) ) {
				removePeerConnection( peerName );
			}
		}, true );

		// Ensure any remaining peers are connected
		variables.peerConnections.each( (peerName, peer)=>{
			if( !peer.isConnectionOpen() ) {
				removePeerConnection( peerName, false );
			}
		}, true );

		// Connect to any new peers that are configured but not yet connected
		currentPeers.each( (peerName)=>{
			ensurePeer( peerName );
		}, true );

		// This will ensure we have a valid manager
		getManagerNode();
	}

	/**
	 * Get the list of configured cluster peers
	 */
	function getPeers() {
		var peers = duplicate( config.cluster.peers ?: [] );
		
		peers.append( getCachePeers(), true );
		// Filter out our own name if present
		return peers
			.toList( chr(10) )
			.ListRemoveDuplicates( chr(10) )
			.listToArray( chr(10) )
			.filter( (peer)=> len( peer ) && peer != myPeerName );
	}

	/**
	 * Get the raw contents of peers from the cache provider.
	 * This is a string delimited by newlines.
	 * Each line is a peer name.
	 * This will return an empty string if there is no cache provider or no peers are found.
	 * 
	 * @return A string of peer names delimited by newlines
	 */
	String function getCachePeersRaw() {
		if( !hasCacheProvider() ) {
			return "";
		}
		var cachedPeers = variables.cacheProvider.get( cacheKeyPrefix );
		if( isNull( cachedPeers ) ) {
			return "";
		}
		return trim( cachedPeers );
	}

	/**
	 * Get the list of peers from the cache provider
	 * @return An array of peer names
	 */
	Array function getCachePeers() {
		return getCachePeersRaw().listToArray( chr(13) & chr(10) ).map( ( peer )=>trim( peer ) );
	}

	/**
	 * Ensure that we are registered in the cache provider as a peer.
	 * Only call this if you have a cache provider configured.
	 * Since the cache provider API can't ensure atomic writes, we will use a basic retry mechanism.
	 * If after the specified number of attempts we are still not present in the cache, we give up.
	 * But no worries, we'll try again next time.
	 * 
	 * @param attempts The number of attempts to add ourselves to the cache before giving up.  Default is 5.
	 * 
	 * @return true if we added successfully or were already present, false if we failed to add ourselves
	 */
	function ensureMyselfInCache( numeric attempts=5 ) {
		updateCacheLastCheckin();
		var cachedPeers = "";
		var i = 1;
		while ( i <= attempts ) {
			cachedPeers = getCachePeersRaw();
			if ( cachedPeers contains myPeerName ) {
				if( i > 1 ) println("SocketBox success adding self to cache!");
				return;
			}
			// Add ourselves
			cachedPeers = cachedPeers.listAppend( myPeerName, chr(13) & chr(10) );
			println("SocketBox added itself to cache attempt #i#." );
			variables.cacheProvider.set( cacheKeyPrefix, cachedPeers );

			// Pause 1-3 seconds to allow for other in-process writes to complete
			sleep( randRange( 1000, 3000 ) );

			// Double check our work in the next loop iteration
			i++;
		}
	}

	/**
	 * Remove a peer from the cache provider.
	 * Only call this if you have a cache provider configured.
	 * 
	 * @param peerName The name of the peer to remove from the cache.
	 * @param attempts The number of attempts to remove the peer before giving up. Default is 5.
	 * 
	 */
	function removedPeerFromCache( required string peerName, numeric attempts=3 ) {
		var cacheKey = cacheKeyPrefix & "-" & peerName;
		variables.cacheProvider.clear( cacheKey );

		var cachedPeers = "";
		var i = 1;
		while ( i <= attempts ) {
			cachedPeers = getCachePeersRaw();
			if ( !(cachedPeers contains peerName) ) {
				if( i > 1 ) println("SocketBox Success removing peer [#peerName#] from cache!");
				return;
			}
			// Remove the peer
			var peersArray = cachedPeers.listToArray( chr(13) & chr(10) ).filter( (peer) => trim(peer) != peerName );
			cachedPeers = peersArray.toList( chr(13) & chr(10) );
			println("SocketBox removed peer [#peerName#] from cache attempt #i#." );
			variables.cacheProvider.set( cacheKeyPrefix, cachedPeers );

			// Pause 1-3 seconds to allow for other in-process writes to complete
			sleep( randRange( 1000, 3000 ) );

			// Double check our work in the next loop iteration
			i++;
		}
	}

	/**
	 * Add a new peer connection to the cluster, but only if it does not already exist.
	 * This will lock the struct to ensure no other thread is adding the same peer at the same time.
	 * @param peerName The name of the peer to connect to
	 */
	function ensurePeer( required string peerName ) {
		if( peerName == myPeerName ) {
			return; // Don't connect to ourselves
		}
		if( !variables.peerConnections.keyExists( peerName ) ) {
			// If the lock timesout, just ignore.  We'll get it next time
			lock name="add_peer_#peerName#" type="exclusive" timeout=20 throwontimeout=false {
				if( !variables.peerConnections.keyExists( peerName ) ) {
					addPeer( peerName );
				}
			}
		}
	}

	/**
	 * Add a new peer connection to the cluster.  I do not check if the peer already exists and I don't lock the struct.
	 * Only call me if you are sure the peer does not exist and you are providing concurrency externally.  
	 * ensurePeer() does this.
	 * @param peerName The name of the peer to connect to
	 */
	function addPeer( required string peerName ) {
		println("SocketBox Connecting to cluster peer: " & peerName);
		try {
			var httpClient = createObject("java", "java.net.http.HttpClient").newHttpClient();
			var javaURI = createObject("java", "java.net.URI").create( peerName );

			var peer = new ClusterPeer( socketbox, this, peerName );

			var timeUnit = createObject("java", "java.util.concurrent.TimeUnit");
			httpClient.newWebSocketBuilder()
				// Authorization header
				.header(
					"socketbox-management",
					config.cluster.secretKey
				)
				// For self-identification
				.header(
					"socketbox-management-name",
					myPeerName
				)
				.buildAsync(
					javaURI,
					createDynamicProxy(
						peer,
						[ "java.net.http.WebSocket$Listener" ]
					)
				)
				.get(config.cluster.peerConnectionTimeoutSeconds, timeUnit.SECONDS)
		} catch( any e ) {
			// If there are issues connecting to peers, then log this as the cluster updating to keep our manager thread running quickly until things settle down
			clusterUpdated();

			// Special message for a few specific cases

			// generic timeout
			if( e.type contains "timeout") {
				println("SocketBox Timeout connecting to cluster peer [#peerName#]");
				return;
			}

			// unresovled hostname
			if( e.stacktrace contains "UnresolvedAddressException" ) {
				println("SocketBox Cannot resolve host for cluster peer [#peerName#]");
				return;
			}

			if( e.stacktrace contains "ConnectException" ) {
				println("SocketBox Connection error connecting to cluster peer [#peerName#]");
				return;
			}

			// generic connection error
			println("SocketBox Error connecting to cluster peer [#peerName#]: " & e.message);
			//println( e.stackTrace )
			return;
		}

		peerConnections[peerName] = peer;

		// println("SocketBox connected to cluster node: " & peerName);
		clusterUpdated();
	}

	/**
	 * Remove a dead peer connection
	 * @param peerName The name of the peer to remove
	 * @param close Whether to close the connection or not
	 */
	function removePeerConnection( required string peerName, boolean close=true ) {
		var existed = variables.peerConnections.keyExists( peerName );
		if( close ) {
			// May not exist.  Handle without locking
			var peer = peerConnections[peerName] ?: "";
			if( !isSimpleValue( peer ) ) {
				// If already closed, should not error per spec
				peer.close();
			}			
		}
		// Won't fail if not exists
		peerConnections.delete( peerName );

		if( existed ) {
			clusterUpdated();
		}
	}

	/**
	 * Do we have a cache provider configured?
	 */
	function hasCacheProvider() {
		return variables.cacheProviderExists;
	}

	/**
	 * Shutdown the cluster manager
	 */
	function shutdown() {
		// This will signal to the manager thread to stop
		variables.clusterManagerKey = "";

		// If we're in cluster mode, remove ourselves from the cache
		if( hasCacheProvider() ) {
			println("SocketBox - Removing myself from cache...");
			// Don't try too hard here.  Another node will eventually flush this.
			// I've found that while testing I often times have a node start right up after shutting down and it's
			// already tryingn to add itself while another node is still trying to remove it.
			// No use fighting over it.
			removedPeerFromCache( myPeerName, 2 );
		}

		shutdownPeerConnections();

	}

	/**
	 * Shutdown all peer connections
	 */
	function shutdownPeerConnections() {
		println("SocketBox Shutting down [#peerConnections.count()#] management connections");
		peerConnections.each( (peerName,clusterPeer)=>{
			try {
				clusterPeer.close()
			} catch( any e ) {
				println("SocketBox Error closing management connection [#peerName#]: " & e.message);
			}
		} );
	}

	/**
	 * Call this any time the state of the cluster changes
	 */
	function clusterUpdated() {
		variables.lastUpdateTick = getTickCount();
		variables.delaySeconds = 2;
	}

	/**
	 * Send an RPC request to a peer in the cluster.
	 * This will block until the response is received or the timeout is reached.
	 * @peerName The name of the peer to send the request to
	 * @operation The name of the operation to call on the peer
	 * @args The arguments to pass to the operation
	 * @timeoutSeconds The number of seconds to wait for the response before timing out.
	 * @defaultValue The value to return if the operation times out or fails
	 * 
	 * @return The result of the operation, or the default value if the operation times out
	 */
	function RPCRequest( required string peerName, required string operation, struct args={}, numeric timeoutSeconds=config.cluster.defaultRPCTimeoutSeconds, any defaultValue ) {
		var peer = variables.peerConnections[peerName] ?: '';
		if( isSimpleValue( peer ) ) {
			if( !isNull( defaultValue ) ) {
				return defaultValue;
			} else {
				throw( type="PeerNotFound", message="Peer [#peerName#] not found in cluster." );
			}
		}
		var RPCID = createUUID();
		var message = serializeJSON( {
			"operation" : operation,
			"peerName" : getMyPeerName(),
			"args" : args,
			"id" : RPCID
		} );

		// Setup the container to hold the RPC operation state
		RPCOperations[RPCID] = {
			"done" : false,
			"thread" : "",
			"result" : "",
			"startTick" : getTickCount(),
			// Used below to detect race conditions
			"original" : true
		};
		// Fire off the RPC message to the peer
		peer.sendText( '__rpc_request__' & message );
		// Start up a thread to wait for the RPC response
		// This thread will be interrupted when the RPC response is received
		// or will timeout after the specified number of seconds.
		var threadName = "rpc_waiter_" & RPCID;
		cfthread( name=threadName, action="run", RPCID="#RPCID#", timeoutSeconds="#timeoutSeconds#") {
			// Set our current thread so the response can interrupt us
			RPCOperations[RPCID].thread = jThread.currentThread();

			// Unless we've already completed, wait for the response
			if( !RPCOperations[RPCID].done ?: true ) {
				sleep( timeoutSeconds * 1000 );
			}
		}
		// Back in our main thread, we can wait for the RPC operation to complete
		// This will block until the thread completes or the timeout is reached
		cfthread( action="join", name=threadName );
		// If the RPC operation is done, return the result
		if( RPCOperations[RPCID].done ) {
			var result = RPCOperations[RPCID].result;
			RPCOperations.delete( RPCID );
			return result;
		} else if( !isNull( defaultValue ) ) {
			RPCOperations.delete( RPCID );
			return defaultValue;
		} else {
			// If the RPC operation is not done, throw a timeout error
			RPCOperations.delete( RPCID );
			throw( type="RPCTimeout", message="RPC operation [#operation#] to peer [#peerName#] timed out after [#timeoutSeconds#] seconds." );
		}
	}
	
	/**
	 * Handle an RPC response from a peer.
	 * This will be called by the peer when it responds to an RPC request.
	 * @data The data received from the peer, which should be a JSON string containing the operation result
	 */
	function onRPCResponse( required string data ) {
		var response = deserializeJSON( data );
		var RPCID = response.id;

		// Check if we have a matching RPC operation
		if( !structKeyExists( RPCOperations, RPCID ) ) {
			// it already timed out, so just ignore this response
			return;
		}

		// Track how long it took
		response[ 'executionTimeMS' ] = getTickCount() - ( RPCOperations[RPCID].startTick ?: 0 );

		// Mark the operation as done and store the result
		RPCOperations[RPCID].result = response;
		RPCOperations[RPCID].done = true;
		var theThread = RPCOperations[RPCID].thread ?: '';
		if( !isSimpleValue( theThread ) ) {
			// Interrupt the thread waiting for the response
			theThread.interrupt();
		}

		// if the "original" key doesn't exist, then we crossed paths just after our check above.  The timeout already happened
		// and cleaned up the RPC operation, and we just re-created worthless keys just now.  Clean and go home.
		// Elvis to avoid race conditions if the ID doesn't exist
		var tmp = RPCOperations[RPCID] ?: {};
		if( !tmp.keyExists( "original" ) ) {
			// Remove the operation from the map
			RPCOperations.delete( RPCID );
		}
	}

	/**
	 * Call this to recalc update delay based on last change
	 * If there have been recent changes, it will set a lower delay, but
	 * back off as the cluster stabilizes.
	 * 
	 * 2 seconds if the last update was less than 10 seconds ago,
	 * 5 seconds if it was less than 30 seconds ago,
	 * 10 seconds if it was less than 60 seconds ago,
	 * 30 seconds if it was less than 5 minutes ago,
	 * and 60 seconds if it was more than 5 minutes ago.
	 * 
	 */
	function recalcUpdateDelay() {
		var secondsSinceLastUpdate = (getTickCount() - variables.lastUpdateTick)/1000;
		// To prevent clusters which all come online at the same time from all trying to update at once,
		// introduce a slight random variation of 0-2 seconds to reduce contention.
		var random = randRange( 0, 2 );

		if( secondsSinceLastUpdate <= 10 ) {
			variables.delaySeconds = 2 + random;
		} else if( secondsSinceLastUpdate <= 30 ) {
			variables.delaySeconds = 5 + random;
		} else if( secondsSinceLastUpdate <= 60 ) {
			variables.delaySeconds = 10 + random;
		} else if( secondsSinceLastUpdate <= 300 ) {
			variables.delaySeconds = 30 + random;
		} else {
			variables.delaySeconds = 60 + random;
		}
		nextRunTick = getTickCount() + (variables.delaySeconds * 1000);
	}

	/**
	 * Shim for println()
	 */
	private function println( required message ) {
		writedump( var=message.toString(), output="console" );
	}

}