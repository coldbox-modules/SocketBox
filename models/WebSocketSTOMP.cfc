/**
 * This is the base WebSocket STOMP component that implements a subset of the STOMP 1.2 protocol.
 * https://stomp.github.io/stomp-specification-1.2.html
 * 
 * Use this in conjunction with CommandBox or the BoxLang Miniserver's websocket server.
 * Extend this CFC with a /WebSocket.cfc in your web root.
 */
component extends="WebSocketCore" {
	variables.heartBeatMS = 10000;
	variables.debugMode = false;

	if( variables.debugMode || !structKeyExists( application, "STOMPExchanges" ) ) {
		_configure();			
	}

	/**
	 * A new incoming message has been received.
	 */
	function onMessage( required messageText, required channel ) {
		// PING messages are empty
		if( !len( trim( messageText ) ) ) {
			sendMessage( chr(10), channel );
		} else {
			var message = getMessageParser().deserialize( messageText );
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
					println( "Unknown STOMP command: #message.getCommand()#" );
			}
		}
	}

	

	function onSTOMPConnect( required message, required channel ) { 
		println("new STOMP connection");
		try {
			if( authenticate( message.getHeader("login",""), message.getHeader("passcode",""), message.getHeader("host", "") ) ) {
					getSTOMPConnections()[ channel.hashCode() ] = {
						"channel" : channel,
						"login" : message.getHeader("login",""),
						"connectDate" : now()
					};
					var message = newMessage(
						"CONNECTED",
						{
							"version" : "1.2",
							"heart-beat" : "#variables.heartBeatMS#,#variables.heartBeatMS#",
							"server" : "SocketBox (STOMP)",
							"session" : channel.hashCode()
						} )
						.validate();
					sendMessage( getMessageParser().serialize(message), channel )
			} else {
				sendError( "Invalid login", "Invalid login", channel, message.getHeader( "receipt", "" ) );
			}
		} catch( "STOMP-Authentication-failure" e ) {
			sendError( "Invalid login", e.message, channel, message.getHeader( "receipt", "" ) );
			return;
		}
	}

	function onSTOMPDisconnect( required message, required channel ) {
		println("STOMP client disconnected");
		removeAllSubscriptsionForChannel( channel );
		getSTOMPConnections().delete( channel.hashCode() );
		sendReceipt( message, channel );
	}

	function onSend( required message, required channel ) {
		println("STOMP SEND message received");
		var exchanges = getExchanges();
		var destination = message.getHeader( "destination", "" );
		var exchange = "direct";
		if( listFind( destination, "/" ) ){ 
			exchange = listFirst( destination, "/" );
			destination = listRest( destination, "/" );
		}
		var channelID = channel.hashCode();
		var login = getSTOMPConnections()[ channelID ].login ?: '';
		
		try {
			if( !authorize( login, exchange, destination, "write" ) ) {
				sendError( "Authorization failure", "Login [#login#] is not authorized with write access to the destination [#exchange#/#destination#]", channel, message.getHeader( "receipt", "" ) );
				return;
			}	
		} catch( "STOMP-Authorization-failure" e ) {
			sendError( "Authorization Failure", e.message, channel, message.getHeader( "receipt", "" ) );
			return;
		}

		if( structKeyExists( exchanges, exchange ) ) {
			exchanges[ exchange ].routeMessage( this, message );
		}
		sendReceipt( message, channel );
	}

	function onSubscribe( required message, required channel ) {
		println("STOMP SUBSCRIBE message received");
		var subscriptionID = message.getHeader( "id" );
		var destination = message.getHeader( "destination" );
		var channelID = channel.hashCode();
		var login = getSTOMPConnections()[ channelID ].login ?: '';
		try {
			if( !authorize( login, "", destination, "read" ) ) {
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
			cflock( name="WebSocketSTOMP-STOMPSubscriptions-#destination#", type="exclusive", timeout=10 ) {
				if( !structKeyExists( subs, destination ) ) {
					subs[ destination ] = {};
				}
			}
		}
		
		subs[ destination ][subscriptionID] = {
			"channel" : channel,
			"channelID" : channel.hashCode(),
			"subscriptionID" : subscriptionID,
			"ack" : ack
		};
		sendReceipt( message, channel );
	}

	function onUnsubscribe( required message, required channel ) {
		println("STOMP UNSUBSCRIBE message received");
		var subscriptionID = message.getHeader( "id" );
		var subs = getSubscriptions();
		var dests = structKeyArray( subs );
		for( var dest in dests ) {
			// Ignored if not exists
			subs[ dest ].delete( subscriptionID );			
		}
		sendReceipt( message, channel );
	}

	function onAck( required message, required channel ) {
		println("STOMP ACK message received");
		var messageID = message.getHeader( "id" );
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	function onNack( required message, required channel ) {
		println("STOMP NACK message received");
		var messageID = message.getHeader( "id" );
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	function onBegin( required message, required channel ) {
		println("STOMP BEGIN message received");
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	function onCommit( required message, required channel ) {
		println("STOMP COMMIT message received");
		var transaction = message.getHeader( "transaction", "" );
		// TODO: Implement
		sendReceipt( message, channel );
	}

	function onAbort( required message, required channel ) {
		println("STOMP ABORT message received");
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
	function authenticate( required string login, required string passcode, string host ) {
		return true;
	}

	/**
	 * Override to implement your own authorization logic
	 */
	function authorize( required string login, required string exchange, required string destination, required string access ) {
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
		if( !structKeyExists( application, "STOMPSubscriptions" ) ) {
			cflock( name="WebSocketSTOMP-STOMPSubscriptions-init", type="exclusive", timeout=10 ) {
				if( !structKeyExists( application, "STOMPSubscriptions" ) ) {
					application.STOMPSubscriptions = {};
				}
			}
		}
		return application.STOMPSubscriptions;
	}

	function getExchanges() {
		if( !structKeyExists( application, "STOMPExchanges" ) ) {
			cflock( name="WebSocketSTOMP-STOMPExchanges-init", type="exclusive", timeout=10 ) {
				if( !structKeyExists( application, "STOMPExchanges" ) ) {
					application.STOMPExchanges = {};
				}
			}
		}
		return application.STOMPExchanges;
	}

	function getSTOMPConnections() {
		if( !structKeyExists( application, "STOMPConnections" ) ) {
			cflock( name="WebSocketSTOMP-STOMPConnections-init", type="exclusive", timeout=10 ) {
				if( !structKeyExists( application, "STOMPConnections" ) ) {
					application.STOMPConnections = {};
				}
			}
		}
		return application.STOMPConnections;
	}

	/**
	 * Get, or intialize the method parser from the application scope
	 */
	function getMessageParser() {
		if( !structKeyExists( application, "WebSocketSTOMPMethodParser" ) ) {
			cflock( name="WebSocketSTOMPMethodParser", type="exclusive", timeout=10 ) {
				if( !structKeyExists( application, "WebSocketSTOMPMethodParser" ) ) {
					application.WebSocketSTOMPMethodParser = new STOMP.MessageParser();
				}
			}
		}
		return application.WebSocketSTOMPMethodParser;
	}

	function newMessage( required string command, struct headers={}, string body="" ) {
		return new STOMP.Message( arguments.command, arguments.headers, arguments.body );
	}

	function _configure() {
		application.WebSocketSTOMPMethodParser = new STOMP.MessageParser();
		var config = configure();
		if( !structKeyExists( local, "config" ) || !isStruct( local.config ) ) {
			throw( type="InvalidConfiguration", message="WebSocket STOMP configure() method must return a struct" );
		}
		var exchanges = getExchanges();
		exchanges[ "direct" ] = new STOMP.exchange.DirectExchange({});
		config.exchanges = config.exchanges ?: {};
		exchanges.append( config.exchanges );
	}

	function configure() {
		// Override me
		return {
			"exchanges" : {
				 /* "direct" : new STOMP.exchange.DirectExchange({})
				 "topic" : new STOMP.exchange.TopicExchange({
				 	  "myTopic.brad" : [
						"destination1"
						"destination2"
					],
					"anotherTopic.*" : [
						"destination3",
						"destination4"
					]
				}),
				"fanout" : new STOMP.exchange.FanoutExchange({
					"destinations" : [
						"destination5",
						"destination6"
					]
				}) */
			}
		};
	}

	function removeAllSubscriptsionForChannel( required channel ) {
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

	function onClose( required channel ) {
		super.onClose( arguments.channel );
		removeAllSubscriptsionForChannel( arguments.channel );
		getSTOMPConnections().delete( channel.hashCode() );
	}

	function send( required string destination, required any messageData ) {
		var exchange = "direct";
		if( listFind( destination, "/" ) ){ 
			exchange = listFirst( destination, "/" );
			destination = listRest( destination, "/" );
		}
		var exchanges = getExchanges();
		if( structKeyExists( exchanges, exchange ) ) {
			exchanges[ exchange ].routeMessage( this, newMessage("SEND", { "destination" : destination }, messageData ) );
		}
	}

}