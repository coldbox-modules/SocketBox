/**
 * This is the base WebSocket STOMP component that implements a subset of the STOMP 1.2 protocol.
 * https://stomp.github.io/stomp-specification-1.2.html
 * 
 * Use this in conjunction with CommandBox or the BoxLang Miniserver's websocket server.
 * Extend this CFC with a /WebSocket.cfc in your web root.
 */
component extends="WebSocketCore" {

	variables.configDefaults = {
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

	function reloadCheck() {
		try {
		// This may just be defaults right now
		var config = application.STOMPBroker.config ?: configDefaults;

		if( config.debugMode || !structKeyExists( application, "STOMPBroker" ) ) {
			cflock( name="WebSocketBrokerInit", type="exclusive", timeout=60 ) {
				if( config.debugMode || !structKeyExists( application, "STOMPBroker" ) ) {
					_configure();
				}
			}
		}
		} catch( any e ) {			
			println( e );
		}
	}

	remote function onProcess() {
		reloadCheck();
		super.onProcess( argumentCollection=arguments );
	}
	/**
	 * A new incoming message has been received.
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

	

	function onSTOMPConnect( required message, required channel ) { 
		logMessage("new STOMP connection");
		try {
			if( authenticate( message.getHeader("login",""), message.getHeader("passcode",""), message.getHeader("host", ""), channel ) ) {
					var sessionID = channel.hashCode();
					getSTOMPConnections()[ channel.hashCode() ] = {
						"channel" : channel,
						"login" : message.getHeader("login",""),
						"connectDate" : now(),
						"sessionID" : sessionID
					};
					var message2 = newMessage(
						"CONNECTED",
						{
							"version" : "1.2",
							"heart-beat" : "#getConfig().heartBeatMS#,#getConfig().heartBeatMS#",
							"server" : "SocketBox (STOMP)",
							"session" : sessionID
						} )
						.validate();
					sendMessage( getMessageParser().serialize(message2), channel )
			} else {
				sendError( "Invalid login", "Invalid login", channel, message.getHeader( "receipt", "" ) );
			}
		} catch( "STOMP-Authentication-failure" e ) {
			sendError( "Invalid login", e.message, channel, message.getHeader( "receipt", "" ) );
			return;
		}
	}

	function onSTOMPDisconnect( required message, required channel ) {
		logMessage("STOMP client disconnected");
		removeAllSubscriptionsForChannel( channel );
		getSTOMPConnections().delete( channel.hashCode() );
		sendReceipt( message, channel );
	}

	function onSend( required message, required channel ) {
		logMessage("STOMP SEND message received");
		var destination = message.getHeader( "destination", "" );
		var parsedDest = parseDestination( destination );
		var channelID = channel.hashCode();
		var login = getSTOMPConnections()[ channelID ].login ?: '';
		
		try {
			if( !authorize( login, parsedDest.exchange, parsedDest.destination, "write", channel ) ) {
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

	function onSubscribe( required message, required channel ) {
		logMessage("STOMP SUBSCRIBE message received");
		var subscriptionID = message.getHeader( "id" );
		var destination = message.getHeader( "destination" );
		var channelID = channel.hashCode();
		var login = getSTOMPConnections()[ channelID ].login ?: '';
		try {
			if( !authorize( login, "", destination, "read", channel ) ) {
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

	function onAck( required message, required channel ) {
		logMessage("STOMP ACK message received");
		var messageID = message.getHeader( "id" );
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	function onNack( required message, required channel ) {
		logMessage("STOMP NACK message received");
		var messageID = message.getHeader( "id" );
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	function onBegin( required message, required channel ) {
		logMessage("STOMP BEGIN message received");
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	function onCommit( required message, required channel ) {
		logMessage("STOMP COMMIT message received");
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	function onAbort( required message, required channel ) {
		logMessage("STOMP ABORT message received");
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	function sendReceipt( required message, required channel ) {
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
	 * Override to implement your own authentication logic
	 */
	boolean function authenticate( required string login, required string passcode, string host, required channel ) {
		return true;
	}

	/**
	 * Override to implement your own authorization logic
	 */
	boolean function authorize( required string login, required string exchange, required string destination, required string access, required channel ) {
		return true;
	}

	/**
	 * Send error
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
		sleep( 1000 );
		// STOMP protocol requires channel to be closed on error
		channel.close();
	}

	function getSubscriptions() {
		reloadCheck();
		return application.STOMPBroker.STOMPSubscriptions;
	}

	function getExchanges() {
		reloadCheck();
		return application.STOMPBroker.STOMPExchanges;
	}

	function getSTOMPConnections() {
		reloadCheck();
		return application.STOMPBroker.STOMPConnections;
	}

	function getConfig() {
		reloadCheck();
		return application.STOMPBroker.config;
	}

	/**
	 * Get, or intialize the method parser from the application scope
	 */
	function getMessageParser() {
		reloadCheck();
		return application.STOMPBroker.WebSocketSTOMPMethodParser;
	}

	function newMessage( required string command, struct headers={}, any body="" ) {
		return new STOMP.Message( arguments.command, arguments.headers, arguments.body );
	}

	// do not call an methods inside here that call reloadChecks() or you'll get a stack overflow!
	function _configure() {
		application.STOMPBroker = {
			WebSocketSTOMPMethodParser = new STOMP.MessageParser(),
			// Don't blow away subscriptions if debugmode is on
			STOMPSubscriptions = application.STOMPBroker.STOMPSubscriptions ?: {},
			STOMPExchanges = {},
			// Don't blow away connections if debugmode is on
			STOMPConnections = application.STOMPBroker.STOMPConnections ?: {},
			config = configDefaults
		};
		var config = configure();
		// Add in defaults
		config.append( configDefaults, false );

		if( !structKeyExists( local, "config" ) || !isStruct( local.config ) ) {
			throw( type="InvalidConfiguration", message="WebSocket STOMP configure() method must return a struct" );
		}
		application.STOMPBroker.config = local.config;
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
		
	}

	function configure() {
		// Override me
		return configDefaults;
	}

	function removeAllSubscriptionsForChannel( required channel ) {
		var channelID = channel.hashCode();
		var subs = getSubscriptions();
		for( var destinationID in subs ) {
			var dest = subs[ destinationID ];
			for( var subscriptionID in dest ) {
				if( dest[ subscriptionID ].channelID == channelID ) {
					dest.delete( subscriptionID );
				}
			}
		}
	}

	function removeAllInternalSubscriptions(required struct subs) {
		for( var destinationID in subs ) {
			var dest = subs[ destinationID ];
			for( var subscriptionID in dest ) {
				if( dest[ subscriptionID ].type == "internal" ) {
					dest.delete( subscriptionID );
				}
			}
		}
	}

	function onClose( required channel ) {
		super.onClose( arguments.channel );
		removeAllSubscriptionsForChannel( arguments.channel );
		getSTOMPConnections().delete( channel.hashCode() );
	}

	function send( required string destination, required any messageData, struct headers={} ) {
		reloadCheck();		
		routeMessage( destination, newMessage("SEND", headers, messageData ) );
	}

	function routeMessage( required string destination, required Message message ) {
		var parsedDest = parseDestination( destination );
		var exchanges = getExchanges();
		if( structKeyExists( exchanges, parsedDest.exchange ) ) {
			exchanges[ parsedDest.exchange ].routeMessage( this, parsedDest.destination, message );
		}
	}

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

	Struct function getConnectionDetails( required channel ) {
		return getSTOMPConnections()[ channel.hashCode() ] ?: {};
	}

	private function logMessage( required any message ) {
		if( getConfig().debugMode ) {
			println( arguments.message );
		}
	}

}