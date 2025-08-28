/**
 * This is the base WebSocket core component that is used to handle WebSocket connections.
 * Use this in conjunction with CommandBox or the BoxLang Miniserver's websocket server.
 * Extend this CFC with a /WebSocket.cfc in your web root.
 */
component {
	detectServerType();

	variables.configDefaults = {
		// reloads config every request if debugMode is true
		"debugMode" : false,
		// Operate in cluster mode, where more than one socketbox server can behave as a single cluster
		"cluster" : {
			// Enable cluster mode.
			"enable" : false,
			// THis can be any string-- a GUID is fine.  Just generate it once and ensure all servers in the cluster use the same secret key.
			"secretKey" : "",
			// The address for OTHER servers in the cluster to connect to this server.  It needs to be accessible by other servers in the cluster and 
			// should use the HTTP port that the server is listening on.  Note, if this server is behind a proxy or load balancer, you need to provide
			// an INTERNAL address and/or port that the other servers in the cluster can connect to directly which doesn't flow through the proxy.
			// Defaults to the server's hostname and the HTTP port in use.
			"name" : "ws://#createObject("java", "java.net.InetAddress").getLocalHost().getHostName()#:#cgi.server_port#/ws",
			// Hard-coded list of cluster peers to connect to. These are always used regardless of external cache.
			"peers" : [],
			// A class or object with MINIMUM get(), set(), and clear() methods to use as a cache provider.
			// A CacheBox provider is perfect for this and offers disk, JDBC, CF/Lucee, Couchbase, Redis, or Memcached backends.
			// You can also provide your own custom cache provider as long as it has get(), set(), and clear() methods.
			// Note, get() must return null if the key is not found.
			// ALL nodes in the cluster MUST share the same external data store.  This cache will be used for registration and discovery of the other nodes in the cluster
			"cacheProvider" : "",
			// Use this if more than one SocketBox cluster is sharing the same backend cache provider.  This prefix will be added to add cache keys
			"cachePrefix" : "",
			// How long to wait, in seconds, while attempting to connect to a peer in the cluster.  A slow network connection may require a longer timeout so we don't give up too soon,
			// but waiting too long may be a waste of time if the peer is truly offline.
			"peerConnectionTimeoutSeconds" : 5,
			// How long to wait, in seconds, before considering a peer idle and kicking it out of the cluster.  This only applies to peers discovered via the shared cache, and the idle time
			// is determined by the epoch date each peer updates in their cache key while they are online.  A longer time is more forgiving for a peer experiencing a short network outtage,
			// but a longer timeout may be a waste of resources if the peer is truly offline and everyone is still trying to connect to it.
			"peerIdleTimeoutSeconds" : 60,
			// How long to wait, in seconds, for an RPC request to timeout waiting for a reply.  If there is a defaultValue specified, it will be returne din the event of a timeout.  
			// If there is no defaultValue specified, then a timeout error will be thrown.
			"defaultRPCTimeoutSeconds" : 15
		}
	};
	
	/**
	 * Front controller for all WebSocket incoming messages
	 */
	remote function onProcess( string WSMethod="" ) {
		reloadCheck();
		try {
			var currentExchange = application.socketBox.serverClass.getCurrentExchange();
			var methodArgs = currentExchange.getAttachment( application.socketBox.WEBSOCKET_REQUEST_DETAILS ) ?: [];
			var realArgs = [];
			// Adobe's argumentCollection doesn't work with a java.util.List :/
			for( var arg in methodArgs ) {
				realArgs.append( arg );
			}
			switch( WSMethod ) {
				case "onConnect":
					if( isManagementConnection( currentExchange, realArgs[1] ) ) {
						_onManagementConnect( argumentCollection=realArgs );
					} else {
						_onConnect( argumentCollection=realArgs );
					}
					break;
				case "onFullTextMessage":
					if( isManagementConnection( currentExchange, realArgs[2] ) ) {
						_onManagementMessage( argumentCollection=realArgs );
					} else {
						_onMessage( argumentCollection=realArgs );
					}
					break;
				case "onClose":
					if( isManagementConnection( currentExchange, realArgs[1] ) ) {
						_onManagementClose( argumentCollection=realArgs );
					} else {
						_onClose( argumentCollection=realArgs );
					}
					break;
				default:
					logMessage("SocketBox Unknown method: #WSMethod#");
			}
		} catch (any e) {			
			println( e );
			rethrow;
		}
	}

	/**
	 * Detect if an incoming connection is a management connection based on the original HTTP headers.
	 * 
	 * @exchange The current exchange
	 * @return boolean True if this is a management connection, false otherwise
	 */
	function isManagementConnection( required any exchange, required any channel ) {
		// If clustering is disabled, then no connections are management connections
		if( !isClusterEnabled() ) {
			return false;
		}

		var channelHash = channel.hashcode();
		if( application.socketBoxClusterManagement.selfChannels.keyExists( channelHash ) ) {
			return true;
		}
		if( application.socketBoxClusterManagement.managementChannels.keyExists( channelHash ) ) {
			return true;
		}
		if( application.socketBoxClusterManagement.channels.keyExists( channelHash ) ) {
			return false;
		}
		
		var headers = exchange.getRequestHeaders();
		var authHeader = headers.get( "socketbox-management" );
		var nameHeader = headers.get( "socketbox-management-name" );

		if( !isNull( authHeader ) && !authHeader.isEmpty() ) {
			var authHeaderValue = authHeader.getFirst();
			// Let's do a case sensitive compare here
			if( len( authHeaderValue ) && compare( authHeaderValue, getConfig().cluster.secretKey ) == 0 ) {
				// If this connection is from myself, then add it to struct of self channels so I can ignore messages from it later
				if( !isNull( nameHeader ) && !nameHeader.isEmpty() ) {
					var nameHeaderValue = nameHeader.getFirst();
					application.socketBoxClusterManagement.managementChannels[ channelHash ] = nameHeaderValue;
					if( len( nameHeaderValue ) && nameHeaderValue == getConfig().cluster.name ) {
						application.socketBoxClusterManagement.selfChannels[ channelHash ] = channel;
					}
				}
				return true;
			}
		}
		return false;
	}

	/**
	 * Override this method to configure the STOMP broker
	 */
	function configure() {
		// Override me
		return {};
	}

	/**
	 * Internal configuration.
	 * Do not call any methods inside here that call reloadChecks() or you'll get a stack overflow!
	 */
	Struct function _configure() {
		logMessage( "SocketBox configuring..." );
		try {
			// Get config and add in defaults
			var config = mergeData( duplicate( configDefaults ), configure() );

			if( !structKeyExists( local, "config" ) || !isStruct( local.config ) ) {
				throw( type="InvalidConfiguration", message="WebSocket configure() method must return a struct" );
			}
			application.SocketBoxConfig = local.config;

			// Setup the cluster if enabled
			if( local.config.cluster.enable ) {
				var oldManager = application.socketBoxClusterManagement.clusterManager ?: '';
				application.socketBoxClusterManagement = {
					"clusterManager" : new cluster.ClusterManager( this, config ),
					// Incoming connections from regular clients
					"channels" : {},
					// Incoming connections from cluster peers
					"managementChannels" : {},
					// Track connections coming from myself so I can ignore them.  This should never happen if my
					// cluster name is configured correctly.
					"selfChannels" : {}
				};

				// Don't let existing connections leak
				if( !isSimpleValue( oldManager ) ) {
					application.socketBoxClusterManagement.clusterManager.setPeerConnections( oldManager.getPeerConnections() );
				}
				
				// Start the cluster manager once the config is fully set up
				application.socketBoxClusterManagement.clusterManager.start()
			}
		} catch( any e ) {
			println( "SocketBox error during configuration: " & e.message );
			// Remove any config we created so we don't leave SocketBox in a corrupted state
			application.delete( 'SocketBoxConfig' );
			if( local.config.cluster.enable ?: false ) {
				application.delete( 'socketBoxClusterManagement' );
				// This will cause the cluster manager thread to exit
				server.socketBoxManagers[getSocketBoxKey()] = '';
			}
			rethrow;
		} 
		return local.config;
	}

	function shutdown() {
		if( !isNull( application.socketBoxClusterManagement ) ) {
			application.socketBoxClusterManagement.clusterManager.shutdown();
		}
	}

	/**
	 * Called every request internally to ensure socketbox is configured
	 */
	function reloadCheck() {
		// This may just be defaults right now
		var config = application.SocketBoxConfig ?: configDefaults;
		
		if( !isInitted( config ) ) {
			cflock( name="SocketBoxInit", type="exclusive", timeout=60 ) {
				if( !isInitted( config ) ) {
					_configure();
				}
			}
		}
	}

	/**
	 * Check if the WebSocket core has been initialized
	 * @return boolean
	 */
	boolean function isInitted( required struct config ) {
		return structKeyExists( application, "SocketBoxConfig" ) && ( !config.cluster.enable || structKeyExists( application, "socketBoxClusterManagement" ));
	}

	/**
	 * Get the current configuration
	 */
	function getConfig() {
		reloadCheck();
		return application.SocketBoxConfig;
	}

	/**********************************
	 * CONNECTION LIFECYCLE METHODS
	 **********************************/


	/**
	 * A new incoming connection has been established.  DDon't override this method.
	 * 
	 * @channel The channel the connection was received on
	 */
	function _onConnect( required channel ) {
		application.socketBoxClusterManagement.channels[ channel.hashcode() ] = channel;
		onConnect( argumentCollection=arguments );
	}

	/**
	 * A connection has been closed.  Don't override this method.
	 * 
	 * @channel The channel the connection was closed on
	 */
	function _onClose( required channel ) {
		application.socketBoxClusterManagement.channels.delete( channel.hashcode() );
		onClose( argumentCollection=arguments );
	}
	/**
	 * A new incoming connection has been established.  Override this to handle a new connection.
	 * 
	 * @channel The channel the connection was received on
	 */
	function onConnect( required channel ) {
		// override me
	}

	/*
	 * A connection has been closed.  Override this to handle a closed connection.
	 * 
	 * @channel The channel the connection was closed on
	 */
	function onClose( required channel ) {
		// override me
	}

	/**
	 * A new incoming management connection has been established.  Don't override this method.
	 * 
	 * @channel The channel the connection was received on
	 */
	function _onManagementConnect( required channel ) {
		var channelHash = channel.hashcode();
		if( application.socketBoxClusterManagement.selfChannels.keyExists( channelHash ) ) {
			// Ignore connections from myself
			return;
		}
		
		// This channel hash is added to application.socketBoxClusterManagement.managementChannels when the connection is first authenticated
		var peerName = application.socketBoxClusterManagement.managementChannels[ channelHash ] ?: "";

		// Immediately ensure a connection back to this peer
		application.socketBoxClusterManagement.clusterManager.ensurePeer( peerName );

		// Tell our friends about this new peer.  They'll discover it soon enough, but it could take up to 60 seconds
		// for their cluster manager to notice.
		broadcastManagementMessage( '__peer_discovered__' & peerName, peerName );

		onManagementConnect( argumentCollection=arguments );
	}

	/**
	 * A Management connection has been closed.  Don't override this method.
	 * 
	 * @channel The channel the connection was closed on
	 */
	function _onManagementClose( required channel ) {
		var channelHash = channel.hashcode();
		application.socketBoxClusterManagement.selfChannels.delete( channelHash );
		application.socketBoxClusterManagement.managementChannels.delete( channelHash );

		// Don't re-broadcast this to the other peers.  Our connection could be a fluke and I'd rather err on the side of staying connected to a peer if unsure.

		onManagementClose( argumentCollection=arguments );
	}

	/**
	 * A new incoming Management connection has been established.  Override this to handle a new connection.
	 * 
	 * @channel The channel the connection was received on
	 */
	function onManagementConnect( required channel ) {
		// override me
	}

	/**
	 * A Management connection has been closed.  Override this to handle a closed connection.
	 * 
	 * @channel The channel the connection was closed on
	 */
	function onManagementClose( required channel ) {
		// override me
	}

	/**
	 * Get all connection.  If in cluster mode, this will not include management connections.  
	 */
	function getAllConnections() {
		if( isClusterEnabled() ) {
			return application.socketBoxClusterManagement.channels.valueArray();	
		} else {
			// Turn Java primitive array into a CF array
			return [].append( getWSHandler().getConnections().toArray(), true );
		}		
	}

	/**
	 * Get all management connections, wrapped in a ClusterPeer instance.  If not in cluster mode, this will throw an exception.
	 */
	function getAllManagementConnections() {
		if( !isClusterEnabled() ) {
			throw( type="ClusterDisabled", message="Cluster mode is not enabled. Cannot get management connections." );
		}
		return application.socketBoxClusterManagement.clusterManager.getPeerConnections().valueArray();
	}

	/**********************************
	 * INCOMING MESSAGE METHODS
	 **********************************/

	/**
	 * A new incoming message has been received.  Don't override this method.
	 * 
	 * @message The message text
	 * @channel The channel the message was received on
	 */
	private function _onMessage( required message, required channel ) {
		var messageText = message;
		// Backwards compat for first iteration of websocket listener
		if( !isSimpleValue( messageText ) ) {
			messageText = message.getData();
		}
		onMessage( messageText, channel );
	}

	/**
	 * A new incoming message has been received.  Override this method.
	 * 
	 * @message The message text
	 * @channel The channel the message was received on
	 */
	function onMessage( required message, required channel ) {
		// Override me
	}

	/**
	 * A new incoming Management message has been received.  Don't override this method.
	 * 
	 * @message The message text
	 * @channel The channel the message was received on
	 */
	private function _onManagementMessage( required message, required channel ) {
		if( application.socketBoxClusterManagement.selfChannels.keyExists( channel.hashcode() ) ) {
			// Ignore messages from myself
			return;
		}

		var messageText = message;
		// Backwards compat for first iteration of websocket listener
		if( !isSimpleValue( messageText ) ) {
			messageText = message.getData();
		}

		// If a message has come in for rebroadcast, then broadcast it to all connected channels
		if( left( messageText, 23 ) == '__message_rebroadcast__' ) {
			broadcastMessage( messageText.right( -23 ), false );
		}
		
		// Rumours of a new peer?  Let's check it out.
		if( left( messageText, 19 ) == '__peer_discovered__' ) {
			application.socketBoxClusterManagement.clusterManager.ensurePeer( messageText.right( -19 ) );
		}

		// If we've received an RPC request.  Let's process it, and reply
		if( left( messageText, 15 ) == '__rpc_request__' ) {
			var rpcData = deserializeJSON( messageText.right( -15 ) );
			try {
				_onRPCRequest( argumentCollection=rpcData ) ;
			} catch( any e ) {
				return sendRPCResponse( rpcData.id, rpcData.peerName, "", false, e );
			}
		}

		// If we've received an RPC response, then pass it to the cluster manager to be reunited with the original request		
		if( left( messageText, 16 ) == '__rpc_response__' ) {
			application.socketBoxClusterManagement.clusterManager.onRPCResponse( messageText.right( -16 ) );
		}

		onManagementMessage( messageText, channel );
	}

	/**
	 * Convenience method to send an RPC request to a specific peer in the cluster.
	 * @peerName The name of the peer to send the request to
	 * @operation The operation to call on the peer
	 * @args The arguments to pass to the operation
	 * @timeoutSeconds The number of seconds to wait for a response
	 * @defaultValue The default value to return if the request times out or fails
	 * @return The response from the peer, or the default value if the request fails or times out
	 */
	function RPCRequest( required string peerName, required string operation, struct args={}, numeric timeoutSeconds, any defaultValue ) {
		if( !isClusterEnabled() ) {
			throw( type="ClusterDisabled", message="Cluster mode is not enabled. Cannot send RPC request." );
		}
		return application.socketBoxClusterManagement.clusterManager.RPCRequest( argumentCollection=arguments );
	}
	
	/**
	 * Send RPC request to all connected peers in the cluster asynchronously.
	 * 
	 * @operation The operation to call on each peer
	 * @args The arguments to pass to the operation
	 * @timeoutSeconds The number of seconds to wait for a response
	 * @defaultValue The default value to return if the request times out or fails
	 */
	function RPCClusterRequest( required string operation, struct args={}, numeric timeoutSeconds, any defaultValue ) {
		if( !isClusterEnabled() ) {
			throw( type="ClusterDisabled", message="Cluster mode is not enabled. Cannot send RPC request." );
		}
		var theArguments = arguments;
		return application.socketBoxClusterManagement.clusterManager.getPeerConnections()
			.map( (peerName,peerConnection) => {
				try {
					return application.socketBoxClusterManagement.clusterManager.RPCRequest( peerName=peerConnection.getPeerName(), argumentCollection=theArguments );
				} catch( any e ) {
					return {
						"success" : false,
						"error" : e,
						"result" : ""
					};
				}
			}, true );
	}

	/**
	 * A new incoming RPC request has been received.  Allow the backend to claim it before a user-specified method is called.
	 * Return true if the request was handled, false otherwise.
	 * 
	 * @operation The operation to call on the peer
	 * @args The arguments to pass to the operation
	 * @id The unique ID of the request
	 * @peerName The name of the peer that sent the request
	 * 
	 * @return boolean True if the request was handled, false otherwise
	 */
	Boolean function _onRPCRequest( required string operation, required struct args, required string id, required string peerName ) {
		switch( operation ) {
			case "uptime" :
				return sendRPCResponse( id, peerName, int( application.socketBoxClusterManagement.clusterManager.getStartTick()/1000 ) );
		}
		return onRPCRequest( operation, args, id, peerName );
	}

	/**
	 * A new incoming RPC request has been received.  Override this method to handle RPC requests.
	 * This method should return a boolean indicating whether the request was handled or not.
	 * 
	 * @operation The operation to call on the peer
	 * @args The arguments to pass to the operation
	 * @id The unique ID of the request
	 * @peerName The name of the peer that sent the request
	 * 
	 * @return boolean True if the request was handled, false otherwise
	 */
	Boolean function onRPCRequest( required string operation, required struct args, required string id, required string peerName ) {
		// Override me to implement more RPC operations
	}

	/**
	 * Use me to send an RPC response back to a peer
	 * @id The unique ID of the request
	 * @peerName The name of the peer to send the response to
	 * @response The response to send back
	 * 
	 * @return Boolean True if the response was sent successfully.
	 */
	Boolean function sendRPCResponse( required string id, required string peerName, any response, boolean success=true, any error={} ) {
		if( !isClusterEnabled() ) {
			throw( type="ClusterDisabled", message="Cluster mode is not enabled. Cannot send RPC response." );
		}
		application.socketBoxClusterManagement.clusterManager.ensurePeer( peerName );
		var data = {
			"id" : id,
		 	"result" : response,
			"success" : success,
			"error" : error
		};
		var message = '__rpc_response__' & serializeJSON( data );
		var peer = application.socketBoxClusterManagement.clusterManager.getPeerConnections()[ peerName ] ?: '';
		if( !isSimpleValue( peer ) ) {
			peer.sendText( message );
		}		
		return true;
	}


	/**
	 * A new incoming Management message has been received.  Override this method.
	 * 
	 * @message The message text
	 * @channel The channel the message was received on
	 */
	function onManagementMessage( required message, required channel ) {
		// Override me
	}

	/**********************************
	 * OUTGOING MESSAGE METHODS
	 **********************************/

	/**
	 * Send a message to a specific channel
	 * 
	 * @message The message text
	 * @channel The channel to send the message to
	 */
	function sendMessage( required message, required channel ) {
		getWSHandler().sendMessage( channel, message );
	}

	/**
	 * Broadcast a message to all connected channels.  This does not include management channels if in cluster mode.
	 * 
	 * @message The message text
	 * @rebroadcast Whether to rebroadcast the message to other cluster peers.
	 */
	function broadcastMessage( required message, boolean rebroadcast=true ) {
		if( isClusterEnabled() ) {
			// When in cluster mode, I don't want "normal" message broadcasts to go to the management channels, so I only send to the regular channels
			// which we track manually.
			getAllConnections().each( (channel) => {
				sendMessage( message=message, channel=channel );
			}, true );
			if( rebroadcast ) {
				broadcastManagementMessage( '__message_rebroadcast__' & message );
			}
		} else {
			// When not in cluster mode, all channels are "normal" and we don't bother tracking them manually, so just delegate to the WebSocket handler
			getWSHandler().broadcastMessage( message );
		}
	}

	/**
	 * Broadcast a Management message to all connected channels.
	 * 
	 * @message The message text
	 * @excludePeer A peer to exclude from the broadcast.  Leave empty for none.
	 */
	function broadcastManagementMessage( required message, excludePeer="" ) {
		if( !isClusterEnabled() ) {
			throw( type="ClusterDisabled", message="Cluster mode is not enabled. Cannot broadcast management message." );
		}
		getAllManagementConnections()
			.filter( (peerConnection) => peerConnection.getPeerName() != excludePeer )
			.each( (peerConnection) => {
				peerConnection.sendText( message );
			}, true );
	}

	/**********************************
	 * UTILITY METHODS
	 **********************************/


	/**
	 * Convenience method to get if cluster mode is enabled
	 */
	function isClusterEnabled() {
		reloadCheck();
		return getConfig().cluster.enable;
	}

	 /**
	  * Get Undertow WebSocket handler from the underlying server
	  */
	private function getWSHandler() {
		if( application.socketBox.serverType == "boxlang-miniserver" ) {
			return application.socketBox.serverClass.getWebsocketHandler();
		} else {
			var exchange = application.socketBox.serverClass.getCurrentExchange()
			if( !isNull( exchange ) ) {
				return exchange.getAttachment( application.socketBox.SITE_DEPLOYMENT_KEY ).getWebsocketHandler();
			}
			// If we're in a cfthread, we won't have a "current" exchange in ThreadLocal
			var deployment = application.socketBox.deployment ?: "";
			if( isSimpleValue( deployment ) ) {
				throw( type="WebSocketHandlerNotFound", message="WebSocket handler not found (no deployment name stored)" );
			}
			return deployment.getWebsocketHandler();				
		}
		throw( type="WebSocketHandlerNotFound", message="WebSocket handler not found" );
	}

	/**
	 * Log a message if debug mode is on.
	 * Don't call anything like getConfig() from this method which may, in turn, print out logs
	 */
	function logMessage( required any message ) {
		if( application.socketboxConfig.debugMode ?: false ) {
			println( arguments.message );
		}
	}

	/**
	 * Shim for println()
	 */
	function println( required message ) {
		writedump( var=message.toString(), output="console" );
	}

	/**
	 * Detect if we're on CommandBox or the BoxLang Miniserver
	 */
	private function detectServerType() {
		if( isNull( application.socketBox ) ) {
			cflock( name="socketBox-init", type="exclusive", timeout=60 ) {
				if( isNull( application.socketBox ) ) {
					try {
						application.socketBox = {
							serverClass : createObject('java', 'runwar.Server'),
							SITE_DEPLOYMENT_KEY : createObject('java', 'runwar.undertow.SiteDeploymentManager').SITE_DEPLOYMENT_KEY,
							WEBSOCKET_REQUEST_DETAILS : createObject('java', 'runwar.undertow.WebsocketReceiveListener').WEBSOCKET_REQUEST_DETAILS,
							serverType : "runwar",
							deployment : ""
						};
					} catch( any e ) {
						try {
							application.socketBox = {
								serverClass : createObject('java', 'ortus.boxlang.web.MiniServer'),
								WEBSOCKET_REQUEST_DETAILS : createObject('java', 'ortus.boxlang.web.handlers.WebsocketReceiveListener' ).WEBSOCKET_REQUEST_DETAILS,
								serverType : "boxlang-miniserver"
							};
						} catch( any e) {
							throw( type="ServerTypeNotFound", message="This websocket library can only run in CommandBox or the BoxLang Miniserver." );
						}
					}
				}
			}
		}
		// This song and dance is because threads don't have access to the thread local variables to get the current deploy, so we want to capture it when we have a chance for later.
		if( application.socketBox.serverType == "runwar" && isSimpleValue( application.socketBox.deployment ?: '' ) && !isNull( application.socketBox.serverClass.getCurrentExchange() ) ) {
			application.socketBox.deployment = application.socketBox.serverClass.getCurrentExchange().getAttachment( application.socketBox.SITE_DEPLOYMENT_KEY );
		}
	}

	/**
	 * Used to get a unique key for each socketbox usage on a server
	 */
	function getSocketBoxKey() {
		return getCurrentTemplatePath();
	}

	
	/**
	* Merges data from source into target.  Target is modified by reference, and also returned from the method for chaining.
	*
	* @target The target object to merge data into
	* @source The source object to merge data from
	*/
	function mergeData( any target, any source ) {

		// If it's a struct...
		if( isStruct( source ) && isStruct( target ) ) {
			// Loop over and process each key
			for( var key in source ) {
				var value = source[ key ];
				if( isSimpleValue( value ) || isObject( value ) || isCustomFunction( value ) ) {
					target[ key ] = value;
				} else if( isStruct( value ) ) {
					target[ key ] = target[ key ] ?: {};
					if( !isStruct( target[ key ] ) ) {
						target[ key ] = {};
					}
					mergeData( target[ key ], value )
				} else if( isArray( value ) ) {
					target[ key ] = target[ key ] ?: [];
					mergeData( target[ key ], value )
				}
			}
		// If it's an array...
		} else if( isArray( source ) && isArray( target ) ) {
			var i=0;
			for( var value in source ) {
				if( !isNull( value ) ) {
					// For arrays, just append them into the target without overwriting existing items
					target.append( value );
				}
			}
		}
		return target;
	}
	
}