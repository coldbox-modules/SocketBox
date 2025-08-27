/**
 * This is the base WebSocket STOMP component that implements a subset of the STOMP 1.2 protocol.
 * https://stomp.github.io/stomp-specification-1.2.html
 * 
 * Use this in conjunction with CommandBox or the BoxLang Miniserver's websocket server.
 * Extend this CFC with a /WebSocket.cfc in your web root.
 * 
 * Since remote method re-create the CFC on every request, no state is maintained between requests.
 * All state is stored in the application scope.
 */
component extends="WebSocketCore" {

	variables.STOMPconfigDefaults = {
		"heartBeatMS" : 10000,
		"debugMode" : false,
		"exchanges" : {
			 /*
			 "direct" : {
				 "bindings" : {
					 "destination1" : "destination2",
					"destination3" : "/topic/foo.bar"
				 }
			 },
			 "topic" : {
				 "bindings" : {
					"myTopic.brad.##" : "destination1",
					"anotherTopic.*" : "fanout/myFanout"
				}
			},
			"fanout" : {
				"bindings" : {
					"myFanout" : [
						"destination1",
						"direct/destination2"
					],
					"anotherFanout" : [
						"destination3",
						"topic/destination4"
					]
				}
			},
			"distribution" : {
				"type" : "random", // roundrobin
				"bindings" : {
					"myDistribution" : [
						"destination1",
						"direct/destination2"
					]
				}
			}
			*/
		},
		"subscriptions" : {
			/*
			"destination1" : (message)=>{
				logMessage( message.getBody() );
			},
			"destination2" : ()=>{}
			*/
		}
	};

	/**
	 * Check if the WebSocket core has been initialized
	 * @return boolean
	 */
	boolean function isInitted( required Struct config ) {
		return super.isInitted( config ) && structKeyExists( application, "STOMPBroker" );
	}

	/**
	 * A new incoming message has been received.  Let's parse it and give it the STOMP treatment.
	 * 
	 * @messageText The message object
	 * @channel The channel the message came from
	 */
	function onMessage( required messageText, required channel ) {
		// PING messages are empty
		if( !len( trim( messageText ) ) ) {
			//logMessage("Received PING message");
			sendMessage( chr(10), channel );
		} else {
			var message = getMessageParser().deserialize( messageText, channel );
			//logMessage("Received #message.getCommand()# message");
			switch( message.getCommand() ) {
				case "CONNECT":
				case "STOMP":
					onSTOMPConnect( message, channel );
					break;
				case "DISCONNECT":
					onSTOMPDisconnect( message, channel );
					break;
				case "SEND":
					onSend( message, channel );
					break;
				case "SUBSCRIBE":
					onSubscribe( message, channel );
					break;
				case "UNSUBSCRIBE":
					onUnsubscribe( message, channel );
					break;
				case "ACK":
					onAck( message, channel );
					break;
				case "NACK":
					onNack( message, channel );
					break;
				case "BEGIN":
					onBegin( message, channel );
					break;
				case "COMMIT":
					onCommit( message, channel );
					break;
				case "ABORT":
					onAbort( message, channel );
					break;
				default:
					logMessage( "Unknown STOMP command: #message.getCommand()#" );
			}
		}
	}

	/**
	 * Handle a new STOMP connection.  If you override this method, make sure you call super.onSTOMPConnect() to ensure the connection is properly established.
	 *
	 * @message The connect message object
	 * @channel The channel the message was received on
	 */
	function onSTOMPConnect( required message, required channel ) { 
		logMessage("new STOMP connection");
		try {
			// Pass this by reference into the authentication function
			var connectionMetadata = {};
			if( authenticate( message.getHeader("login",""), message.getHeader("passcode",""), message.getHeader("host", ""), channel, connectionMetadata ) ) {
					var sessionID = channel.hashCode();
					getSTOMPConnections()[ channel.hashCode() ] = {
						"channel" : channel,
						"login" : message.getHeader("login",""),
						"connectDate" : now(),
						"sessionID" : sessionID,
						"connectionMetadata" : connectionMetadata
					};
					// Setup default headers
					var headers = {
							"version" : "1.2",
							"heart-beat" : "#getConfig().heartBeatMS#,#getConfig().heartBeatMS#",
							"server" : "SocketBox (STOMP)",
							"session" : sessionID,
							"host" : getConfig().cluster.name ?: "<unknown>"
						};
					
						// Mix in our connection metadata with a prefix
					connectionMetadata.each( (k,v)=> headers[ 'connectionMetadata-' & k ] = v );

					var message2 = newMessage( "CONNECTED", headers ).validate();

					sendMessage( getMessageParser().serialize(message2), channel )
			} else {
				sendError( "Invalid login", "Invalid login", channel, message.getHeader( "receipt", "" ) );
			}
		} catch( "STOMP-Authentication-failure" e ) {
			sendError( "Invalid login", e.message, channel, message.getHeader( "receipt", "" ) );
			return;
		}
	}

	/**
	 * Called when a STOMP client disconnects.  If you override this method, make sure you call super.onSTOMPDisconnect() to ensure the connection is properly closed.
	 *
	 * @message The disconnect message
	 * @channel The channel the message was received on
	 */
	function onSTOMPDisconnect( required message, required channel ) {
		logMessage("STOMP client disconnected");
		removeAllSubscriptionsForChannel( channel );
		getSTOMPConnections().delete( channel.hashCode() );
		sendReceipt( message, channel );
	}

	/**
	 * Handle a new STOMP message.  If you override this method, make sure you call super.onSend() to ensure the message is properly routed.
	 *
	 * @message the message object
	 * @channel the channel the message was received on
	 */
	function onSend( required message, required channel ) {
		logMessage("STOMP SEND message received");
		var destination = message.getHeader( "destination", "" );
		var parsedDest = parseDestination( destination );
		var channelID = channel.hashCode();
		var login = getSTOMPConnections()[ channelID ].login ?: '';
		var connectionMetadata = getSTOMPConnections()[ channelID ].connectionMetadata ?: {};
		
		try {
			if( !authorize( login, parsedDest.exchange, parsedDest.destination, "write", channel, connectionMetadata ) ) {
				sendError( "Authorization failure", "Login [#login#] is not authorized with write access to the destination [#parsedDest.exchange#/#parsedDest.destination#]", channel, message.getHeader( "receipt", "" ) );
				return;
			}	
		} catch( "STOMP-Authorization-failure" e ) {
			sendError( "Authorization Failure", e.message, channel, message.getHeader( "receipt", "" ) );
			return;
		}

		routeMessage( destination, message );
		sendReceipt( message, channel );
	}

	/**
	 * Handle a new STOMP subscription.  If you override this method, make sure you call super.onSubscribe() to ensure the subscription is properly established.
	 *
	 * @message The message object
	 * @channel The channel the message came from
	 */
	function onSubscribe( required message, required channel ) {
		logMessage("STOMP SUBSCRIBE message received");
		var subscriptionID = message.getHeader( "id" );
		var destination = message.getHeader( "destination" );
		var channelID = channel.hashCode();
		var login = getSTOMPConnections()[ channelID ].login ?: '';
		var connectionMetadata = getSTOMPConnections()[ channelID ].connectionMetadata ?: {};
		try {
			// Check if the user is authorized to read from the destination
			if( !authorize( login, "", destination, "read", channel, connectionMetadata ) ) {
				sendError( "Authorization failure", "Login [#login#] is not authorized with read access to the destination [#destination#]", channel, message.getHeader( "receipt", "" ) );
				return;
			}	
		} catch( "STOMP-Authorization-failure" e ) {
			sendError( "Authorization Failure", e.message, channel, message.getHeader( "receipt", "" ) );
			return;
		}
		var ack = message.getHeader( "ack", "auto" );
		var subs = getSubscriptions();
		if( !structKeyExists( subs, destination ) ) {
			cflock( name="WebSocketSTOMP-STOMPSubscriptions-#destination#", type="exclusive", timeout=60 ) {
				if( !structKeyExists( subs, destination ) ) {
					subs[ destination ] = {};
				}
			}
		}
		
		subs[ destination ][channelID & ":" & subscriptionID] = {
			"type" : "channel",
			"channel" : channel,
			"channelID" : channel.hashCode(),
			"subscriptionID" : subscriptionID,
			"ack" : ack,
			"callback" : ""
		};
		sendReceipt( message, channel );
	}

	/**
	 * Handle a new STOMP unsubscription.  If you override this method, make sure you call super.onUnsubscribe() to ensure the subscription is properly removed.
	 *
	 * @message The message object
	 * @channel The channel the message came from
	 */
	function onUnsubscribe( required message, required channel ) {
		logMessage("STOMP UNSUBSCRIBE message received");
		var channelID = channel.hashCode();
		var subscriptionID = message.getHeader( "id" );
		var subs = getSubscriptions();
		var dests = structKeyArray( subs );
		for( var dest in dests ) {
			// Ignored if not exists
			subs[ dest ].delete( channelID & ":" & subscriptionID );			
		}
		sendReceipt( message, channel );
	}

	/**
	 * Handle a new STOMP ACK message.  If you override this method, make sure you call super.onAck() to ensure the message is properly acknowledged.
	 *
	 * @message The message object
	 * @channel The channel the message came from
	 */
	function onAck( required message, required channel ) {
		logMessage("STOMP ACK message received");
		var messageID = message.getHeader( "id" );
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	/**
	 * Handle a new STOMP NACK message.  If you override this method, make sure you call super.onNack() to ensure the message is properly acknowledged.
	 *
	 * @message The message object
	 * @channel The channel the message came from
	 */
	function onNack( required message, required channel ) {
		logMessage("STOMP NACK message received");
		var messageID = message.getHeader( "id" );
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	/**
	 * Handle a new STOMP BEGIN message.  If you override this method, make sure you call super.onBegin() to ensure the transaction is properly started.
	 *
	 * @message The message object
	 * @channel The channel the message came from
	 */
	function onBegin( required message, required channel ) {
		logMessage("STOMP BEGIN message received");
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	/**
	 * Handle a new STOMP COMMIT message.  If you override this method, make sure you call super.onCommit() to ensure the transaction is properly committed.
	 *
	 * @message The message object
	 * @channel The channel the message came from
	 */
	function onCommit( required message, required channel ) {
		logMessage("STOMP COMMIT message received");
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	/**
	 * Handle a new STOMP ABORT message.  If you override this method, make sure you call super.onAbort() to ensure the transaction is properly aborted.
	 * 
	 * @message The message object
	 * @channel The channel the message came from
	 */
	function onAbort( required message, required channel ) {
		logMessage("STOMP ABORT message received");
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	/**
	 * Override to implement your own authentication logic
	 * 
	 * @login The login name supplied by the client
	 * @passcode The passcode supplied by the client
	 * @host The host name supplied by the client
	 * @channel The channel the client is connecting through
	 * @connectionMetadata Additional metadata about the connection for you to populate which will be sent back to the client via the headers of the CONNECTED frame.
	 */
	boolean function authenticate( required string login, required string passcode, string host, required channel, required Struct connectionMetadata ) {
		return true;
	}

	/**
	 * Override to implement your own authorization logic
	 * 
	 * @login The login name supplied by the client
	 * @exchange The exchange the client is trying to access
	 * @destination The destination the client is trying to access
	 * @access The access level being requested
	 * @channel The channel the client is connecting through
	 * @connectionMetadata The metadata you set in the authenticate() method about the connection.  Changes will be persisted and passed to any subsequent authorize() calls.  
	 */
	boolean function authorize( required string login, required string exchange, required string destination, required string access, required channel, required Struct connectionMetadata ) {
		return true;
	}

	/**
	 * Send a message from the server side to all subscribers of a destination
	 * 
	 * @destination The destination to send the message to
	 * @messageData The data to include in the message
	 * @headers Any additional headers to include with the message
	 * @rebroadcast Whether to rebroadcast the message to other cluster nodes
	 */
	function send( required string destination, required any messageData, struct headers={}, boolean rebroadcast=true ) {
		reloadCheck();		
		routeMessage( destination, newMessage("SEND", headers, messageData ), rebroadcast );
	}

	/**
	 * Get all subscriptions
	 */
	function getSubscriptions() {
		reloadCheck();
		return application.STOMPBroker.STOMPSubscriptions;
	}

	/**
	 * Get all exchanges
	 */
	function getExchanges() {
		reloadCheck();
		return application.STOMPBroker.STOMPExchanges;
	}

	/**
	 * Get all STOMP connections
	 */
	function getSTOMPConnections() {
		reloadCheck();
		return application.STOMPBroker.STOMPConnections;
	}

	/**
	 * Get all STOMP connections across the entire cluster
	 */
	function getClusterSTOMPConnections() {
		reloadCheck();
		var results = mapSTOMPConnections();
		if( isClusterEnabled() ) {
			RPCClusterRequest( 'getSTOMPCConnections' ).each( (peerName, peerResponse ) => {
				if( peerResponse.success ) {
					results.append( peerResponse.result, true );
				}
			});
		}
		return results;
	}

	/**
	 * Map the struct of STOMP connections to an array of data suitable to transfer via text
	 */
	private function mapSTOMPConnections() {
		return application.STOMPBroker.STOMPConnections.reduce( (connections, key, value)=>{
			if( value.channel.isOpen() ) {
				connections.append({
					"login" : value.login,
					"connectDate" : value.connectDate,
					"sessionID" : value.sessionID,
					"server" : getConfig().cluster.name
				});
			} else {
				println( "removing dead channel for [#value.login#] from mapSTOMPConnections." )
				removeAllSubscriptionsForChannel( value.channel );
				getSTOMPConnections().delete( value.channel.hashCode() );
			}
			return connections;
		}, [] );
		
	}

	/**
	 * Get the connection details for a given channel
	 *
	 * @channel The channel to get the connection details for
	 */
	Struct function getConnectionDetails( required channel ) {
		return getSTOMPConnections()[ channel.hashCode() ] ?: {};
	}

	/**
	 * Get, or intialize the method parser from the application scope
	 */
	function getMessageParser() {
		reloadCheck();
		return application.STOMPBroker.WebSocketSTOMPMethodParser;
	}

	/**
	 * Create a new STOMP message
	 */
	function newMessage( required string command, struct headers={}, any body="" ) {
		return new STOMP.Message( arguments.command, arguments.headers, arguments.body );
	}

	/**
	 * Internal configuration.
	 * Do not call any methods inside here that call reloadChecks() or you'll get a stack overflow!
	 */
	Struct function _configure() {
		try {
			// Setup core config
			var config = super._configure();
			
			mergeData( config, variables.STOMPconfigDefaults );

			// Add STOMP specific config
			application.STOMPBroker = {
				WebSocketSTOMPMethodParser = new STOMP.MessageParser(),
				// Don't blow away subscriptions if debugmode is on
				STOMPSubscriptions = application.STOMPBroker.STOMPSubscriptions ?: {},
				STOMPExchanges = {},
				// Don't blow away connections if debugmode is on
				STOMPConnections = application.STOMPBroker.STOMPConnections ?: {},
			};
			

			var exchanges = application.STOMPBroker.STOMPExchanges;
			exchanges[ "direct" ] = new STOMP.exchange.DirectExchange({});
			config.exchanges = config.exchanges ?: {};
			exchanges.append( config.exchanges.map( (name,props)=>{
				props = duplicate( props );
				props.class = v.class ?: name;
				props.name = name;
				switch(props.class) {
					case "direct":
						return new STOMP.exchange.DirectExchange( properties=props );
					case "topic":
						return new STOMP.exchange.TopicExchange( properties=props );
					case "fanout":
						return new STOMP.exchange.FanoutExchange( properties=props );
					case "distribution":
						return new STOMP.exchange.DistributionExchange( properties=props );
					default:
						// struct key should be fqn to a CFC
						return createObject( "component", props.class ).init( properties=props )
				}
			} ) );

			// re-create internal subscriptions
			removeAllInternalSubscriptions(application.STOMPBroker.STOMPSubscriptions);
			config.subscriptions = config.subscriptions ?: {};
			var subCounter = 0;
			config.subscriptions.each( (destination, callback)=>{
				var subscriptionID = "internal-" & subCounter++;
				registerInternalSubscription( application.STOMPBroker.STOMPSubscriptions, subscriptionID, destination, callback )
			} );

		} catch( any e ) {
			println( "SocketBox STOMP error during configuration: " & e.message );
			// Remove any config we created so we don't leave SocketBox in a corrupted state
			application.delete( 'STOMPBroker' );
			rethrow;
		} 
		
		return config;
	}

	/**
	 * Handle low level websocket close
	 *
	 * @channel The channel that was closed
	 */
	function onClose( required channel ) {
		super.onClose( arguments.channel );
		removeAllSubscriptionsForChannel( arguments.channel );
		getSTOMPConnections().delete( channel.hashCode() );
	}

	/**
	 * Route a message to the appropriate exchange
	 *
	 * @destination The destination to route the message to in the format exchange/destination
	 * @message The message to route
	 */
	function routeMessage( required string destination, required Message message, boolean rebroadcast=true ) {
		var parsedDest = parseDestination( destination );
		var exchanges = getExchanges();
		if( structKeyExists( exchanges, parsedDest.exchange ) ) {
			// add the publisher-id to the message header, send as 0 if not present ( server side )
			if( !isNull( message.getChannel() ) ) {
				message.setHeader( "publisher-id", message.getChannel().hashCode() ?: 0 );
			}
			exchanges[ parsedDest.exchange ].routeMessage( this, parsedDest.destination, message );
		}
		if( rebroadcast && isClusterEnabled() ) {
			var serializedMessageData = serializeJSON( {
				"destination" : destination,
				"messageData" : message.getBody(),
				"headers" : message.getHeaders()
			} );

			broadcastManagementMessage( '__STOMP_message_rebroadcast__' & serializedMessageData );
		}
	}

	/**
	 * Parse a destination into an exchange and a destination
	 *
	 * @destination The destination to parse in the format exchange/destination or just destination
	 * @return A struct with exchange and destination keys
	 */
	Struct function parseDestination( required string destination ) {
		var result = {
			exchange = "direct",
			destination = destination
		};
		
		if( listLen( destination, "/" ) > 1 ){ 
			result.exchange = listFirst( destination, "/" );
			result.destination = listRest( destination, "/" );
		}
		return result;
	}

	/**
	 * Log a message if debug mode is on
	 */
	private function logMessage( required any message ) {
		if( getConfig().debugMode ) {
			println( arguments.message );
		}
	}

	

	/**
	 * Logic for creating a server-side subscription
	 *
	 * @subs 
	 * @subscriptionID 
	 * @destination 
	 * @callback 
	 */
	private function registerInternalSubscription( required struct subs, required string subscriptionID, required string destination, required callback ) {		
		if( !structKeyExists( subs, destination ) ) {
			cflock( name="WebSocketSTOMP-STOMPSubscriptions-#destination#", type="exclusive", timeout=60 ) {
				if( !structKeyExists( subs, destination ) ) {
					subs[ destination ] = {};
				}
			}
		}
		
		subs[ destination ][subscriptionID] = {
			"type" : "internal",
			"channel" : "",
			"channelID" : "",
			"subscriptionID" : subscriptionID,
			"ack" : "",
			"callback" : callback
		};
	}

	/**
	 * Send a receipt message back to the client
	 */
	private function sendReceipt( required message, required channel ) {
		var receiptID = message.getHeader( "receipt", "" );
		if( len( trim( receiptID ) ) ) {
			var receipt = newMessage(
				"RECEIPT",
				{
					"receipt-id" : receiptID
				}
			).validate();
			sendMessage( getMessageParser().serialize( receipt ), channel );
		}
	}

	/**
	 * Send error back to the client.  Per the STOMP spec, the channel must be closed after an error is sent.
	 */
	function sendError( required string message, string detail=arguments.message, required channel, string receiptID="" ) {
		var headers = {
			"message" : message
		};
		if( len( trim( receiptID ) ) ) {
			headers[ "receipt-id" ] = receiptID;
		}
		var error = newMessage(
			"ERROR",
			headers,
			arguments.detail
		).validate();
		sendMessage( getMessageParser().serialize( error ), channel );
		// Give the client a chance to receive it
		sleep( 1000 );
		// STOMP protocol requires channel to be closed on error
		channel.close();
	}

	/**
	 * Remove all subscriptions for a given channel.  Used when disconnecting to clean up
	 *
	 * @channel The channel to remove subscriptions for
	 */
	private function removeAllSubscriptionsForChannel( required channel ) {
		var channelID = channel.hashCode();
		var subs = getSubscriptions();
		for( var destinationID in subs ) {
			var dest = subs[ destinationID ];
			for( var subscriptionID in dest ) {
				// Elvis is race condition protection since subscription ID can disappear from dest at any time
				if( (dest[ subscriptionID ].channelID ?: '') == channelID ) {
					dest.delete( subscriptionID );
				}
			}
		}
	}

	/**
	 * Remove all internal subscriptions.  Used when reconfiguring to clean up
	 *
	 * @subs The subscriptions struct
	 */
	private function removeAllInternalSubscriptions(required struct subs) {
		for( var destinationID in subs ) {
			var dest = subs[ destinationID ];
			for( var subscriptionID in dest ) {
				if( dest[ subscriptionID ].type == "internal" ) {
					dest.delete( subscriptionID );
				}
			}
		}
	}

	

	/**
	 * A new incoming Management message has been received.  Don't override this method.
	 * 
	 * @message The message
	 * @channel The channel the message came from
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

		// If a message has come in for STOMP rebroadcast, then funnel it into the send() logic for processing
		if( left( messageText, 29 ) == '__STOMP_message_rebroadcast__' ) {
			var serializedMessageData = messageText.right( -29 );
			var messageData = deserializeJSON( serializedMessageData );
			send(
				messageData.destination,
				messageData.messageData,
				messageData.headers,
				false // Don't rebroadcast this message
			);			
		}
		
		super._onManagementMessage(
			messageText,
			channel
		);
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
			case "getSTOMPCConnections" :
				return sendRPCResponse( id, peerName, mapSTOMPConnections() );
		}
		return super._onRPCRequest( arguments.operation, arguments.args, arguments.id, arguments.peerName );
	}

}