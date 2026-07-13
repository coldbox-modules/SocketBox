/**
 * Regression tests for SocketBox cluster resource management.
 * Run with either checked-in CommandBox server config and request /tests/runner.cfm.
 */
component extends="testbox.system.BaseSpec" {

function selectorThreadCount() {
	var count = 0;
	var threadClass = createObject( "java", "java.lang.Thread" );
	for( var threadEntry in threadClass.getAllStackTraces().entrySet() ) {
		var thread = threadEntry.getKey();
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

function newConfig( any cacheProvider="", string name="ws://current:8080/ws" ) {
	return {
		cluster : {
			cachePrefix : "test-",
			cacheProvider : arguments.cacheProvider,
			defaultRPCTimeoutSeconds : 1,
			name : arguments.name,
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
	$assert.isTrue(
		waitForSelectorCount( initialSelectors + 1 ),
		"A cluster manager should create exactly one shared HttpClient selector thread."
	);

	manager.shutdown();
	$assert.isTrue(
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

	$assert.isEqual( 10, webSocketBuilder.connectTimeoutMS, "The timeout must be applied to the WebSocket handshake." );
	$assert.isTrue(
		pendingFuture.cancelled,
		"An unfinished connection future must be cancelled after an error. Logs: #serializeJSON( socketBox.messages )#"
	);
	$assert.isEqual( 0, manager.getPeerConnections().len(), "A failed peer must not be retained." );

	var connectedPeer = { forceClosed : false };
	connectedPeer.close = ( force )=>{
		connectedPeer.forceClosed = force;
	};
	manager.setPeerConnections( { "connected-peer" : connectedPeer } );
	manager.shutdown();
	$assert.isTrue( connectedPeer.forceClosed, "Manager shutdown must force-close established peer WebSockets." );
	$assert.isEqual( 0, manager.getPeerConnections().len(), "Manager shutdown must clear established peers." );
	var javaVersion = val( createObject( "java", "java.lang.System" ).getProperty( "java.specification.version" ) );
	$assert.isEqual(
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

	$assert.isTrue( pendingFuture.cancelled, "Manager shutdown must cancel an in-flight peer handshake." );
	$assert.isEqual( 0, manager.getPeerConnections().len(), "A handshake cancelled by shutdown must not add a peer." );
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

	$assert.isTrue( cacheFailureRaised, "The simulated cache failure should reach the caller." );
	$assert.isTrue( connectedPeer.forceClosed, "A cache failure must not skip peer WebSocket cleanup." );
	$assert.isEqual( 0, manager.getPeerConnections().len(), "A cache failure must not leave retained peers." );
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

	$assert.isTrue( webSocket.closeSent, "A forced peer close should still initiate a close frame." );
	$assert.isTrue( webSocket.aborted, "A forced peer close must abort the WebSocket." );
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

	$assert.isEqual( 1, peerListWrites, "Multiple expired peers should be removed with one peer-list write." );
	$assert.isEqual( 1, remainingPeers.len(), "Only the current peer should remain in the cache list." );
	$assert.isEqual( currentPeer, remainingPeers[ 1 ], "The active peer must be preserved." );
	$assert.isTrue( !cacheData.keyExists( "#cacheKeyPrefix#-#expiredPeerOne#" ), "The first expired heartbeat should be cleared." );
	$assert.isTrue( !cacheData.keyExists( "#cacheKeyPrefix#-#expiredPeerTwo#" ), "The second expired heartbeat should be cleared." );

	manager.shutdown();
}

function testSuccessfulConnectionsReuseSharedHttpClient() {
	var connectionAttempts = [];
	var httpClient = { builderCount : 0, shutdownCount : 0 };
	httpClient.newWebSocketBuilder = ()=>{
		httpClient.builderCount++;
		var attempt = {
			headers : {},
			timeoutMS : 0,
			cancelCount : 0,
			webSocket : {
				closeCount : 0,
				abortCount : 0
			}
		};
		attempt.webSocket.sendClose = ( statusCode, reason )=>{
			attempt.webSocket.closeCount++;
			return { get : ()=>{} };
		};
		attempt.webSocket.abort = ()=>attempt.webSocket.abortCount++;

		var builder = {};
		builder.connectTimeout = ( duration )=>{
			attempt.timeoutMS = duration.toMillis();
			return builder;
		};
		builder.header = ( name, value )=>{
			attempt.headers[ name ] = value;
			return builder;
		};
		builder.buildAsync = ( uri, listener )=>{
			attempt.uri = uri.toString();
			attempt.listener = listener;
			listener.setWebSocket( attempt.webSocket );

			var future = {};
			future.get = ()=>attempt.webSocket;
			future.isDone = ()=>true;
			future.cancel = ( mayInterruptIfRunning )=>{
				attempt.cancelCount++;
				return true;
			};
			connectionAttempts.append( attempt );
			return future;
		};
		return builder;
	};
	httpClient.shutdownNow = ()=>httpClient.shutdownCount++;

	var manager = newClusterManager(
		newSocketBoxMock(),
		newConfig(),
		httpClient,
		( peer )=>peer
	);
	manager.setDelaySeconds( 60 );

	manager.addPeer( "ws://peer-one:8080/ws" );
	manager.addPeer( "ws://peer-two:8080/ws" );

	$assert.isEqual( 2, httpClient.builderCount, "Both connections must use the manager's shared HTTP client." );
	$assert.isEqual( 2, manager.getPeerConnections().len(), "Successful peer connections must be retained." );
	$assert.isEqual( 2, manager.getDelaySeconds(), "A successful connection should reset the adaptive delay." );
	$assert.isEqual( 2, connectionAttempts.len(), "Both successful handshakes should be recorded." );
	for( var attempt in connectionAttempts ) {
		$assert.isEqual( 10, attempt.timeoutMS, "Each handshake must receive the configured timeout." );
		$assert.isEqual( "test-secret", attempt.headers[ "socketbox-management" ], "Each handshake must include the cluster secret." );
		$assert.isEqual( "ws://current:8080/ws", attempt.headers[ "socketbox-management-name" ], "Each handshake must identify the current peer." );
		$assert.isEqual( 0, attempt.cancelCount, "A completed handshake must not be cancelled." );
	}

	manager.shutdown();
	for( var attempt in connectionAttempts ) {
		$assert.isEqual( 1, attempt.webSocket.closeCount, "Shutdown should initiate one close per connected peer." );
		$assert.isEqual( 1, attempt.webSocket.abortCount, "Shutdown should force-abort each connected peer." );
		$assert.isEqual( 0, attempt.cancelCount, "Completed futures must not be retained for shutdown cancellation." );
	}
}

function testShutdownIsIdempotentAndContinuesAfterPeerCloseFailure() {
	var failingPeer = { closeCount : 0 };
	failingPeer.close = ( force )=>{
		failingPeer.closeCount++;
		throw( type="CloseFailure", message="simulated close failure" );
	};
	var healthyPeer = { closeCount : 0, forceClosed : false };
	healthyPeer.close = ( force )=>{
		healthyPeer.closeCount++;
		healthyPeer.forceClosed = force;
	};
	var httpClient = { shutdownCount : 0 };
	httpClient.newWebSocketBuilder = ()=>{};
	httpClient.shutdownNow = ()=>httpClient.shutdownCount++;

	var manager = newClusterManager( newSocketBoxMock(), newConfig(), httpClient );
	manager.setPeerConnections( {
		"failing-peer" : failingPeer,
		"healthy-peer" : healthyPeer
	} );

	manager.shutdown();
	manager.shutdown();

	$assert.isEqual( 1, failingPeer.closeCount, "A failing peer should only be closed once." );
	$assert.isEqual( 1, healthyPeer.closeCount, "A peer failure must not prevent other peers from closing." );
	$assert.isTrue( healthyPeer.forceClosed, "Manager shutdown must force-close healthy peers." );
	$assert.isEqual( 0, manager.getPeerConnections().len(), "Shutdown must clear all peers even when one close fails." );
	var javaVersion = val( createObject( "java", "java.lang.System" ).getProperty( "java.specification.version" ) );
	$assert.isEqual(
		javaVersion >= 21 ? 1 : 0,
		httpClient.shutdownCount,
		"The shared HTTP client should be shut down at most once."
	);
}

function testSuccessfulHandshakeCompletingDuringShutdownIsNotRetained() {
	var manager = "";
	var socket = { closeCount : 0, abortCount : 0 };
	socket.sendClose = ( statusCode, reason )=>{
		socket.closeCount++;
		return { get : ()=>{} };
	};
	socket.abort = ()=>socket.abortCount++;
	var connectionFuture = { cancelCount : 0 };
	connectionFuture.isDone = ()=>true;
	connectionFuture.cancel = ( mayInterruptIfRunning )=>{
		connectionFuture.cancelCount++;
		return true;
	};
	connectionFuture.get = ()=>{
		manager.shutdown();
		return socket;
	};

	var builder = {};
	builder.connectTimeout = ( duration )=>builder;
	builder.header = ( name, value )=>builder;
	builder.buildAsync = ( uri, listener )=>{
		listener.setWebSocket( socket );
		return connectionFuture;
	};
	var httpClient = { shutdownCount : 0 };
	httpClient.newWebSocketBuilder = ()=>builder;
	httpClient.shutdownNow = ()=>httpClient.shutdownCount++;

	manager = newClusterManager(
		newSocketBoxMock(),
		newConfig(),
		httpClient,
		( peer )=>peer
	);
	manager.addPeer( "ws://late-peer:8080/ws" );

	$assert.isEqual( 0, connectionFuture.cancelCount, "A completed future should not be cancelled during shutdown." );
	$assert.isEqual( 1, socket.closeCount, "A peer that completes during shutdown must be closed." );
	$assert.isEqual( 1, socket.abortCount, "A peer that completes during shutdown must be force-aborted." );
	$assert.isEqual( 0, manager.getPeerConnections().len(), "A peer that completes during shutdown must not be retained." );
}

function testCacheRemovalRetriesAfterConflictingWrite() {
	var currentPeer = "ws://current:8080/ws";
	var expiredPeer = "ws://expired:8080/ws";
	var concurrentPeer = "ws://concurrent:8080/ws";
	var cacheKeyPrefix = "test-socketbox-cluster-peers";
	var cacheData = {
		"#cacheKeyPrefix#" : [ expiredPeer, currentPeer ].toList( chr(13) & chr(10) ),
		"#cacheKeyPrefix#-#expiredPeer#" : 1
	};
	var peerListWrites = 0;
	var cacheProvider = {};
	cacheProvider.get = ( key )=>cacheData.keyExists( key ) ? cacheData[ key ] : nullValue();
	cacheProvider.set = ( key, value )=>{
		cacheData[ key ] = value;
		if( key == cacheKeyPrefix ) {
			peerListWrites++;
			if( peerListWrites == 1 ) {
				cacheData[ key ] = [ expiredPeer, currentPeer, concurrentPeer ].toList( chr(13) & chr(10) );
			}
		}
	};
	cacheProvider.clear = ( key )=>{
		var existed = cacheData.keyExists( key );
		cacheData.delete( key );
		return existed;
	};
	var httpClient = { shutdownNow : ()=>{} };
	httpClient.newWebSocketBuilder = ()=>{};
	var manager = newClusterManager( newSocketBoxMock(), newConfig( cacheProvider ), httpClient );

	var removed = manager.removePeersFromCache( [ expiredPeer ], 2 );
	var remainingPeers = manager.getCachePeers();

	$assert.isTrue( removed, "Cache removal should succeed after retrying a conflicting write." );
	$assert.isEqual( 2, peerListWrites, "A conflicting write should force one verified retry." );
	$assert.isFalse( remainingPeers.contains( expiredPeer ), "The expired peer must be absent after the retry." );
	$assert.isTrue( remainingPeers.contains( currentPeer ), "The current peer must be preserved during the retry." );
	$assert.isTrue( remainingPeers.contains( concurrentPeer ), "A concurrently observed active peer must be preserved." );

	manager.shutdown();
}

function testCacheRegistrationUsesExactPeerIdentity() {
	var currentPeer = "ws://node-1:8080/ws";
	var similarPeer = "ws://node-10:8080/ws";
	var cacheKeyPrefix = "test-socketbox-cluster-peers";
	var cacheData = {
		"#cacheKeyPrefix#" : similarPeer
	};
	var cacheProvider = {};
	cacheProvider.get = ( key )=>cacheData.keyExists( key ) ? cacheData[ key ] : nullValue();
	cacheProvider.set = ( key, value )=>cacheData[ key ] = value;
	cacheProvider.clear = ( key )=>{
		var existed = cacheData.keyExists( key );
		cacheData.delete( key );
		return existed;
	};
	var httpClient = { shutdownNow : ()=>{} };
	httpClient.newWebSocketBuilder = ()=>{};
	var manager = newClusterManager(
		newSocketBoxMock(),
		newConfig( cacheProvider, currentPeer ),
		httpClient
	);

	manager.ensureMyselfInCache( 1 );
	var cachedPeers = manager.getCachePeers();

	$assert.isTrue( cachedPeers.contains( currentPeer ), "The current peer must be registered when only a similar name exists." );
	$assert.isTrue( cachedPeers.contains( similarPeer ), "Registering the current peer must preserve the similarly named peer." );
	$assert.isEqual( 2, cachedPeers.len(), "Similar peer names must remain distinct cache entries." );

	manager.shutdown();
}

function testConnectionFailurePreservesAdaptiveDelayUntilSuccess() {
	var buildCount = 0;
	var successfulSocket = { closeCount : 0, abortCount : 0 };
	successfulSocket.sendClose = ( statusCode, reason )=>{
		successfulSocket.closeCount++;
		return { get : ()=>{} };
	};
	successfulSocket.abort = ()=>successfulSocket.abortCount++;
	var httpClient = { shutdownNow : ()=>{} };
	httpClient.newWebSocketBuilder = ()=>{
		buildCount++;
		var builder = {};
		builder.connectTimeout = ( duration )=>builder;
		builder.header = ( name, value )=>builder;
		builder.buildAsync = ( uri, listener )=>{
			var future = {};
			future.isDone = ()=>true;
			future.cancel = ( mayInterruptIfRunning )=>true;
			if( buildCount == 1 ) {
				future.get = ()=>{
					throw( type="java.util.concurrent.TimeoutException", message="test timeout" );
				};
			} else {
				listener.setWebSocket( successfulSocket );
				future.get = ()=>successfulSocket;
			}
			return future;
		};
		return builder;
	};
	var manager = newClusterManager(
		newSocketBoxMock(),
		newConfig(),
		httpClient,
		( peer )=>peer
	);
	manager.setDelaySeconds( 60 );

	manager.addPeer( "ws://retry-peer:8080/ws" );
	$assert.isEqual( 60, manager.getDelaySeconds(), "A failed connection must not reset the adaptive delay." );

	manager.addPeer( "ws://retry-peer:8080/ws" );
	$assert.isEqual( 2, manager.getDelaySeconds(), "A successful connection should reset the adaptive delay." );
	$assert.isEqual( 1, manager.getPeerConnections().len(), "The successful retry should retain the peer." );

	manager.shutdown();
}

}
