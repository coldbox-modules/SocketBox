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
	 * The number of seconds after which a peer is considered expired.
	 * If a peer has not checked in within this time, it is elligible for removal.
	 */
	property name="peerTimeout" type="numeric";

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
		variables.config = config;
		variables.clusterManagerKey = createUUID();
		variables.peerTimeout = 60;
		variables.socketBox = socketBox;
		variables.peerConnections = {};
		variables.lastUpdateTick = getTickCount();
		variables.lockName = "socketbox-cluster-peer-lock-" & createUUID();
		if( !isSimpleValue( config.cluster.cacheProvider ) ) {
			variables.cacheProviderExists = true;
			variables.cacheProvider = config.cluster.cacheProvider;
		} else {
			variables.cacheProviderExists = false;
		}
		variables.cacheKeyPrefix = "#config.cluster.cachePrefix#socketbox-cluster-peers";
		variables.myPeerName = config.cluster.name;
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
			systemOutput( "SocketBox cluster manager thread started." );
			sleep( variables.delaySeconds * 1000 );

			// If the application has restarted and we are no longer the current manager, then this thread is done
			var updated = false;
			while( (server.socketBoxManagers[socketBox.getSocketBoxKey()] ?: '') == variables.clusterManagerKey ) {
				try {
					updated = false;
					updateCacheLastCheckin();
					if( nextRunTick <= getTickCount() ) {
						updated = true;
						systemOutput("Checking SocketBox cluster peers after " & variables.delaySeconds & " seconds...");
						checkPeers();
					}
					// Check for peer connections every delaySeconds
					sleep( 2000 );
				} catch( any e ) {
					systemOutput("SocketBox error in SocketBox cluster manager: " & e.message);
				} finally {
					if( updated ) {
						recalcUpdateDelay();
					}
				}
			}
			systemOutput( "SocketBox cluster manager thread stopped." );
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
	 * Get the current epoch seconds
	 * This is used to store the last checkin time in the cache provider
	 */
	function getEpochSeconds() {	
		return dateDiff("s", createDateTime(1970,1,1,0,0,0), now());
	}

	/**
	 * Check for expired peers in the cache and remove them
	 */
	function reapExpiredCachePeers() {
		if( !hasCacheProvider() ) {
			return;
		}
		getCachePeers().each( (cachePeer)=>{
			// Get this every time so it's fresh
			var nowEpochSeconds = getEpochSeconds();
			var lastCheckin = val( variables.cacheProvider.get( cacheKeyPrefix & "-" & cachePeer ) ?: '' );
			if( lastCheckin < ( nowEpochSeconds - variables.peerTimeout ) ) {
				// This peer has timed out
				systemOutput( "Removing expired peer from cache: " & cachePeer );
				removedPeerFromCache( cachePeer );	
			}
		} );
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
		} );

		// Ensure any remaining peers are connected
		variables.peerConnections.each( (peerName, peer)=>{
			if( !peer.isConnectionOpen() ) {
				removePeerConnection( peerName, false );
			}
		} );		

		// Connect to any new peers that are configured but not yet connected
		currentPeers.each( (peerName)=>{
			ensurePeer( peerName );
		} );
	}

	/**
	 * Get the list of configured cluster peers
	 */
	function getPeers() {
		var peers = duplicate( config.cluster.peers ?: [] );
		
		peers.append( getCachePeers(), true );
		// Filter out our own name if present
		return peers.filter( (peer)=>peer != myPeerName );
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
				if( i > 1 ) SystemOutput("success adding self to cache!");
				return;
			}
			// Add ourselves
			cachedPeers = cachedPeers.listAppend( myPeerName, chr(13) & chr(10) );
			systemOutput("SocketBox added itself to cache attempt #i#." );
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
	function removedPeerFromCache( required string peerName, numeric attempts=5 ) {
		var cacheKey = cacheKeyPrefix & "-" & peerName;
		variables.cacheProvider.clear( cacheKey );

		var cachedPeers = "";
		var i = 1;
		while ( i <= attempts ) {
			cachedPeers = getCachePeersRaw();
			if ( !(cachedPeers contains peerName) ) {
				if( i > 1 ) systemOutput("Success removing peer [#peerName#] from cache!");
				return;
			}
			// Remove the peer
			var peersArray = cachedPeers.listToArray( chr(13) & chr(10) ).filter( (peer) => trim(peer) != peerName );
			cachedPeers = peersArray.toList( chr(13) & chr(10) );
			systemOutput("SocketBox removed peer [#peerName#] from cache attempt #i#." );
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
			lock name="lockName" type="exclusive" timeout=20 throwontimeout=false {
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
		SystemOutput("SocketBox attempting connection to cluster peer: " & peerName);
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
				.get(5, timeUnit.SECONDS) // Timeout after 10 seconds
		} catch( any e ) {
			systemOutput("Error connecting to cluster peer [#peerName#]: " & e.message);
			return;
		}

		peerConnections[peerName] = peer;

		SystemOutput("SocketBox connected to cluster node: " & peerName);
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
			systemOutput("Removing myself from cache...");
			// Don't try too hard here.  Another node will eventually flush this.
			// I've found that while testing I often times have a node start right up after shutting down and it's
			// already tryingn to add itself while another node is still trying to remove it.
			// No use fighting over it.
			removedPeerFromCache( myPeerName, 2 );
		}

		// Shut down all peer connections
		systemOutput("Shutting down [#peerConnections.count()#] management connections");
		peerConnections.each( (peerName,clusterPeer)=>{
			try {
				clusterPeer.close()
			} catch( any e ) {
				systemOutput("Error closing management connection [#peerName#]: " & e.message);
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
	 * Shim for Lucee's systemOutput()
	 */
	private function systemOutput( required message ) {
		writedump( var=message.toString(), output="console" );
	}

}