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
			// Useful for debugging.  Unique name for this server in the cluster.  Defaults to hostname.
			"name" : "ws://#cgi.HTTP_HOST#/ws",
			// The address for OTHER servers in the cluster to connect to this server.  It needs to be accessible by other servers in the cluster and 
			// should use the HTTP port that the server is listening on.  Note, if this server is behind a proxy or load balancer, you need to provide
			// an INTERNAL address and/or port that the other servers in the cluster can connect to directly which doesn't flow through the proxy.
			// Defaults to the server's hostname and the HTTP port in use.
			"myAddress" : "",
			// Instance of a cache provder which supports the get() and put() methods.
			// This is used to store the cluster state and is required for cluster mode to work
			"cacheProvider" : "",
			// Hard-coded list of cluster peers to connect to. These are always used regardless of external cache.
			"peers" : [],
			// A class or object with MINIMUM get() and put() methods to use as a cache provider.
			// A CacheBox provider is perfect for this and offers disk, JDBC, CF/Lucee, Couchbase, Redis, or Memcached backends.
			// You can also provide your own custom cache provider as long as it has get() and put() methods.
			// Note, get() must return null if the key is not found.
			// ALL nodes in the cluster MUST share the same external data store.  This cache will be used for registration and discovery of the other nodes in the cluster
			"cacheProvider" : "",
			// Use this if more than one SocketBox cluster is sharing the same backend cache provider.  This prefix will be added to add cache keys
			"cachePrefix" : ""
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
					println("Unknown method: #WSMethod#");
			}
		} catch (any e) {			
			println( e );
			rethrow;
		}
	}

	/**
	 * Detect if an incoming connection is a management connection based on the original HTTP headers.
	 * 
	 * @param exchange The current exchange
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
		SystemOutput( "SocketBox configuring..." );
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
			systemOutput( "SocketBox error during configuration: " & e.message );
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
		try {
		// This may just be defaults right now
		var config = application.SocketBoxConfig ?: configDefaults;
		
		if( ( config.debugMode && (request.socketBoxReloaded ?: false) ) || !isInitted( config ) ) {
			cflock( name="WebSocketBrokerInit", type="exclusive", timeout=60 ) {
				if( ( config.debugMode && (request.socketBoxReloaded ?: false) ) || !isInitted( config ) ) {
					// Only let debug mode reload once per request
					if( config.debugMode ) {
						request.socketBoxReloaded = true;
					}
					//shutdown();
					_configure();
				}
			}
		}
		} catch( any e ) {
			rethrow;
			println( e );
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
	 * A new incoming connection has been established
	 */
	function _onConnect( required channel ) {
		application.socketBoxClusterManagement.channels[ channel.hashcode() ] = channel;
		onConnect( argumentCollection=arguments );
	}

	/**
	 * A connection has been closed
	 */
	function _onClose( required channel ) {
		application.socketBoxClusterManagement.channels.delete( channel.hashcode() );
		onClose( argumentCollection=arguments );
	}
	/**
	 * A new incoming connection has been established
	 */
	function onConnect( required channel ) {
		// override me
	}

	/**
	 * A connection has been closed
	 */
	function onClose( required channel ) {
		// override me
	}

	/**
	 * A new incoming management connection has been established
	 */
	function _onManagementConnect( required channel ) {
		var channelHash = channel.hashcode();
		if( application.socketBoxClusterManagement.selfChannels.keyExists( channelHash ) ) {
			// Ignore connections from myself
			return;
		}
		
		// This channel hash is added to application.socketBoxClusterManagement.managementChannels when the connection is first authenticated
		var peerName = application.socketBoxClusterManagement.managementChannels[ channelHash ] ?: "";
		if( len( peerName ) ) {
			systemOutput("Management connection established from #peerName#");
		}
		// Immediately ensure a connection back to this peer
		application.socketBoxClusterManagement.clusterManager.ensurePeer( peerName );

		// Tell our friends about this new peer.  They'll discover it soon enough, but it could take up to 60 seconds
		// for their cluster manager to notice.
		broadcastManagementMessage( '__peer_discovered__' & peerName, peerName );

		onManagementConnect( argumentCollection=arguments );
	}

	/**
	 * A Management connection has been closed
	 */
	function _onManagementClose( required channel ) {
		var channelHash = channel.hashcode();
		application.socketBoxClusterManagement.selfChannels.delete( channelHash );
		application.socketBoxClusterManagement.managementChannels.delete( channelHash );

		// Don't re-broadcast this to the other peers.  Our connection could be a fluke and I'd rather err on the side of staying connected to a peer if unsure.

		onManagementClose( argumentCollection=arguments );
	}
	/**
	 * A new incoming Management connection has been established
	 */
	function onManagementConnect( required channel ) {
		// override me
	}

	/**
	 * A Management connection has been closed
	 */
	function onManagementClose( required channel ) {
		// override me
	}

	/**
	 * Get all connections
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
	 * Get all management connections
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
	 */
	function onMessage( required message, required channel ) {
		// Override me
	}

	/**
	 * A new incoming Management message has been received.  Don't override this method.
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

		onManagementMessage( messageText, channel );
	}

	/**
	 * A new incoming Management message has been received.  Override this method.
	 */
	function onManagementMessage( required message, required channel ) {
		// Override me
	}

	/**********************************
	 * OUTGOING MESSAGE METHODS
	 **********************************/

	/**
	 * Send a message to a specific channel
	 */
	function sendMessage( required message, required channel ) {
		//println("sending message to specific channel: #message#");
		getWSHandler().sendMessage( channel, message );
	}

	/**
	 * Broadcast a message to all connected channels
	 */
	function broadcastMessage( required message, boolean rebroadcast=true ) {
		if( isClusterEnabled() ) {
			// When in cluster mode, I don't want "normal" message broadcasts to go to the management channels, so I only send to the regular channels
			// which we track manually.
			getAllConnections().each( (channel) => {
				sendMessage( message=message, channel=channel );
			} );
			if( rebroadcast ) {
				broadcastManagementMessage( '__message_rebroadcast__' & message );
			}
		} else {
			// When not in cluster mode, all channels are "normal" and we don't bother tracking them manually, so just delegate to the WebSocket handler
			getWSHandler().broadcastMessage( message );
		}
	}

	/**
	 * Broadcast a Management message to all connected channels
	 */
	function broadcastManagementMessage( required message, excludePeer="" ) {
		if( !isClusterEnabled() ) {
			throw( type="ClusterDisabled", message="Cluster mode is not enabled. Cannot broadcast management message." );
		}
		getAllManagementConnections()
			.filter( (peerConnection) => peerConnection.getPeerName() != excludePeer )
			.each( (peerConnection) => {
				peerConnection.sendText( message );
			} );
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
	 * Shim for BoxLang's println()
	 */
	private function println( required message ) {
		systemOutput( message, true );
	}

	/**
	 * Shim for Lucee's systemOutput()
	 */
	private function systemOutput( required message ) {
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
						application.socketBox.serverClass = createObject('java', 'runwar.Server')
						application.socketBox.SITE_DEPLOYMENT_KEY = createObject('java', 'runwar.undertow.SiteDeploymentManager' ).SITE_DEPLOYMENT_KEY;
						application.socketBox.WEBSOCKET_REQUEST_DETAILS = createObject('java', 'runwar.undertow.WebsocketReceiveListener' ).WEBSOCKET_REQUEST_DETAILS;
						application.socketBox.serverType = "runwar";
						application.socketBox.deployment = "";
					} catch( any e ) {
						try {
							application.socketBox.serverClass = createObject('java', 'ortus.boxlang.web.MiniServer')
							application.socketBox.WEBSOCKET_REQUEST_DETAILS = createObject('java', 'ortus.boxlang.web.handlers.WebsocketReceiveListener' ).WEBSOCKET_REQUEST_DETAILS;
							application.socketBox.serverType = "boxlang-miniserver";
						} catch( any e) {
							throw( type="ServerTypeNotFound", message="This websocket library can only run in CommandBox or the BoxLang Miniserver." );
						}
					}
				}
			}
		}
		// This song and dance is because threads don't hvae access to the thread local variables to get the current deploy, so we want to capture it when we have a chance for later.
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
	* Merges data from source into target
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