<cfscript>
/**
 * Lightweight regression tests for SocketBox cluster resource management.
 * Run with: box run-script test
 */

function assertTrue( required boolean condition, required string message ) {
	if( !arguments.condition ) {
		throw( type="AssertionFailed", message=arguments.message );
	}
}

function assertEquals( required any expected, required any actual, required string message ) {
	if( arguments.expected != arguments.actual ) {
		throw(
			type="AssertionFailed",
			message="#arguments.message# Expected [#arguments.expected#], actual [#arguments.actual#]."
		);
	}
}

function selectorThreadCount() {
	var count = 0;
	var threadClass = createObject( "java", "java.lang.Thread" );
	for( var thread in threadClass.getAllStackTraces().keySet() ) {
		if( thread.isAlive() && thread.getName().matches( "HttpClient-\d+-SelectorManager" ) ) {
			count++;
		}
	}
	return count;
}

function waitForSelectorCount( required numeric expected, numeric timeoutMS=2000 ) {
	var endTick = getTickCount() + arguments.timeoutMS;
	do {
		if( selectorThreadCount() == arguments.expected ) {
			return true;
		}
		sleep( 25 );
	} while( getTickCount() < endTick );
	return selectorThreadCount() == arguments.expected;
}

function waitForSelectorCountAfterGC( required numeric expected, numeric timeoutMS=5000 ) {
	var jSystem = createObject( "java", "java.lang.System" );
	var endTick = getTickCount() + arguments.timeoutMS;
	do {
		jSystem.gc();
		jSystem.runFinalization();
		if( selectorThreadCount() == arguments.expected ) {
			return true;
		}
		sleep( 50 );
	} while( getTickCount() < endTick );
	return selectorThreadCount() == arguments.expected;
}

function newClusterManager( required any socketBox, required struct config, any httpClient, any peerListenerFactory ) {
	var manager = createObject( "component", "models.cluster.ClusterManager" );

	if( arguments.keyExists( "peerListenerFactory" ) && !isNull( arguments.peerListenerFactory ) ) {
		return manager.init(
			arguments.socketBox,
			arguments.config,
			arguments.httpClient,
			arguments.peerListenerFactory
		);
	}
	if( arguments.keyExists( "httpClient" ) && !isNull( arguments.httpClient ) ) {
		return manager.init( arguments.socketBox, arguments.config, arguments.httpClient );
	}
	return manager.init( arguments.socketBox, arguments.config );
}

function newSocketBoxMock() {
	var mock = { messages : [] };
	mock.logMessage = ( message )=>mock.messages.append( message );
	mock.getSocketBoxKey = ()=>"socketbox-tests";
	return mock;
}

function newConfig( any cacheProvider="" ) {
	return {
		cluster : {
			cachePrefix : "test-",
			cacheProvider : arguments.cacheProvider,
			defaultRPCTimeoutSeconds : 1,
			name : "ws://current:8080/ws",
			peerConnectionTimeoutSeconds : 0.01,
			peerIdleTimeoutSeconds : 60,
			peers : [],
			secretKey : "test-secret"
		}
	};
}

function testSharedHttpClientLifecycle() {
	var javaVersion = val( createObject( "java", "java.lang.System" ).getProperty( "java.specification.version" ) );
	var initialSelectors = selectorThreadCount();
	var manager = newClusterManager( newSocketBoxMock(), newConfig() );
	assertTrue(
		waitForSelectorCount( initialSelectors + 1 ),
		"A cluster manager should create exactly one shared HttpClient selector thread."
	);

	manager.shutdown();
	assertTrue(
		javaVersion >= 21
			? waitForSelectorCount( initialSelectors )
			: waitForSelectorCountAfterGC( initialSelectors ),
		"Cluster manager shutdown should terminate its shared HttpClient selector thread."
	);
}

function testConnectionTimeoutCancellation() {
	var pendingFuture = { cancelled : false };
	pendingFuture.get = ()=>{
		throw( type="java.util.concurrent.TimeoutException", message="test timeout" );
	};
	pendingFuture.isDone = ()=>false;
	pendingFuture.cancel = ( mayInterruptIfRunning )=>{
		pendingFuture.cancelled = true;
		return true;
	};

	var webSocketBuilder = { connectTimeoutMS : 0 };
	webSocketBuilder.connectTimeout = ( duration )=>{
		webSocketBuilder.connectTimeoutMS = duration.toMillis();
		return webSocketBuilder;
	};
	webSocketBuilder.header = ( name, value )=>webSocketBuilder;
	webSocketBuilder.buildAsync = ( uri, listener )=>pendingFuture;

	var httpClient = { shutdownCalled : false };
	httpClient.newWebSocketBuilder = ()=>webSocketBuilder;
	httpClient.shutdownNow = ()=>{
		httpClient.shutdownCalled = true;
	};

	var socketBox = newSocketBoxMock();
	var manager = newClusterManager(
		socketBox,
		newConfig(),
		httpClient,
		( peer )=>peer
	);
	manager.addPeer( "ws://offline:8080/ws" );

	assertEquals( 10, webSocketBuilder.connectTimeoutMS, "The timeout must be applied to the WebSocket handshake." );
	assertTrue(
		pendingFuture.cancelled,
		"An unfinished connection future must be cancelled after an error. Logs: #serializeJSON( socketBox.messages )#"
	);
	assertEquals( 0, manager.getPeerConnections().len(), "A failed peer must not be retained." );

	var connectedPeer = { forceClosed : false };
	connectedPeer.close = ( force )=>{
		connectedPeer.forceClosed = force;
	};
	manager.setPeerConnections( { "connected-peer" : connectedPeer } );
	manager.shutdown();
	assertTrue( connectedPeer.forceClosed, "Manager shutdown must force-close established peer WebSockets." );
	assertEquals( 0, manager.getPeerConnections().len(), "Manager shutdown must clear established peers." );
	var javaVersion = val( createObject( "java", "java.lang.System" ).getProperty( "java.specification.version" ) );
	assertEquals(
		javaVersion >= 21,
		httpClient.shutdownCalled,
		"Explicit HTTP client shutdown should only be used when the JVM provides it."
	);
}

function testShutdownCancelsPendingHandshake() {
	var manager = "";
	var pendingFuture = { cancelled : false };
	pendingFuture.isDone = ()=>pendingFuture.cancelled;
	pendingFuture.cancel = ( mayInterruptIfRunning )=>{
		pendingFuture.cancelled = true;
		return true;
	};
	pendingFuture.get = ()=>{
		manager.shutdown();
		throw( type="java.util.concurrent.CancellationException", message="cancelled by shutdown" );
	};

	var webSocketBuilder = {};
	webSocketBuilder.connectTimeout = ( duration )=>webSocketBuilder;
	webSocketBuilder.header = ( name, value )=>webSocketBuilder;
	webSocketBuilder.buildAsync = ( uri, listener )=>pendingFuture;

	var httpClient = {};
	httpClient.newWebSocketBuilder = ()=>webSocketBuilder;
	httpClient.shutdownNow = ()=>{};

	manager = newClusterManager(
		newSocketBoxMock(),
		newConfig(),
		httpClient,
		( peer )=>peer
	);
	manager.addPeer( "ws://pending:8080/ws" );

	assertTrue( pendingFuture.cancelled, "Manager shutdown must cancel an in-flight peer handshake." );
	assertEquals( 0, manager.getPeerConnections().len(), "A handshake cancelled by shutdown must not add a peer." );
}

function testCacheFailureCannotSkipResourceCleanup() {
	var cacheProvider = {};
	cacheProvider.get = ( key )=>"";
	cacheProvider.set = ( key, value )=>{};
	cacheProvider.clear = ( key )=>{
		throw( type="CacheFailure", message="simulated cache outage" );
	};

	var httpClient = {};
	httpClient.newWebSocketBuilder = ()=>{};
	httpClient.shutdownNow = ()=>{};
	var connectedPeer = { forceClosed : false };
	connectedPeer.close = ( force )=>{
		connectedPeer.forceClosed = force;
	};

	var manager = newClusterManager(
		newSocketBoxMock(),
		newConfig( cacheProvider ),
		httpClient
	);
	manager.setPeerConnections( { "connected-peer" : connectedPeer } );
	var cacheFailureRaised = false;
	try {
		manager.shutdown();
	} catch( CacheFailure e ) {
		cacheFailureRaised = true;
	}

	assertTrue( cacheFailureRaised, "The simulated cache failure should reach the caller." );
	assertTrue( connectedPeer.forceClosed, "A cache failure must not skip peer WebSocket cleanup." );
	assertEquals( 0, manager.getPeerConnections().len(), "A cache failure must not leave retained peers." );
}

function testForceCloseAbortsWebSocket() {
	var clusterManager = {};
	clusterManager.getMyPeerName = ()=>"ws://current:8080/ws";
	var clusterPeer = createObject( "component", "models.cluster.ClusterPeer" )
		.init( newSocketBoxMock(), clusterManager, "ws://peer:8080/ws" );
	var closeFuture = {};
	closeFuture.get = ()=>{
		throw( type="AssertionFailed", message="A forced close must not wait for the graceful close future." );
	};
	var webSocket = { closeSent : false, aborted : false };
	webSocket.sendClose = ( statusCode, reason )=>{
		webSocket.closeSent = true;
		return closeFuture;
	};
	webSocket.abort = ()=>{
		webSocket.aborted = true;
	};
	clusterPeer.setWebSocket( webSocket );

	clusterPeer.close( true );

	assertTrue( webSocket.closeSent, "A forced peer close should still initiate a close frame." );
	assertTrue( webSocket.aborted, "A forced peer close must abort the WebSocket." );
}

function testExpiredPeersAreRemovedInOneBatch() {
	var currentPeer = "ws://current:8080/ws";
	var expiredPeerOne = "ws://expired-one:8080/ws";
	var expiredPeerTwo = "ws://expired-two:8080/ws";
	var cacheKeyPrefix = "test-socketbox-cluster-peers";
	var nowEpoch = int( createObject( "java", "java.lang.System" ).currentTimeMillis() / 1000 );
	var cacheData = {
		"#cacheKeyPrefix#" : [ expiredPeerOne, expiredPeerTwo, currentPeer ].toList( chr(13) & chr(10) ),
		"#cacheKeyPrefix#-#expiredPeerOne#" : nowEpoch - 120,
		"#cacheKeyPrefix#-#expiredPeerTwo#" : nowEpoch - 120,
		"#cacheKeyPrefix#-#currentPeer#" : nowEpoch
	};
	var peerListWrites = 0;
	var cacheProvider = {};
	cacheProvider.get = ( key )=>cacheData.keyExists( key ) ? cacheData[ key ] : nullValue();
	cacheProvider.set = ( key, value )=>{
		cacheData[ key ] = value;
		if( key == cacheKeyPrefix ) {
			peerListWrites++;
		}
	};
	cacheProvider.clear = ( key )=>{
		var existed = cacheData.keyExists( key );
		cacheData.delete( key );
		return existed;
	};

	var httpClient = { shutdownNow : ()=>{} };
	httpClient.newWebSocketBuilder = ()=>{};
	var manager = newClusterManager(
		newSocketBoxMock(),
		newConfig( cacheProvider ),
		httpClient
	);

	manager.reapExpiredCachePeers();
	var remainingPeers = manager.getCachePeers();

	assertEquals( 1, peerListWrites, "Multiple expired peers should be removed with one peer-list write." );
	assertEquals( 1, remainingPeers.len(), "Only the current peer should remain in the cache list." );
	assertEquals( currentPeer, remainingPeers[ 1 ], "The active peer must be preserved." );
	assertTrue( !cacheData.keyExists( "#cacheKeyPrefix#-#expiredPeerOne#" ), "The first expired heartbeat should be cleared." );
	assertTrue( !cacheData.keyExists( "#cacheKeyPrefix#-#expiredPeerTwo#" ), "The second expired heartbeat should be cleared." );

	manager.shutdown();
}

testSharedHttpClientLifecycle();
testConnectionTimeoutCancellation();
testShutdownCancelsPendingHandshake();
testCacheFailureCannotSkipResourceCleanup();
testForceCloseAbortsWebSocket();
testExpiredPeersAreRemovedInOneBatch();
writeOutput( "SocketBox cluster regression tests passed." );
</cfscript>
