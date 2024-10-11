/**
 * Base class for STOMP exchanges
 */
component accessors="true" {
	property name="properties" type="struct";

	function init( struct properties={} ) {
		setProperties( arguments.properties );
		return this;
	}

	function routeMessage( required WebSocketSTOMP STOMPBroker, required string destination, required any message ){
		throw( message="Method not implemented", detail="You must implement the routeMessage method in your exchange", type="MethodNotImplemented" );
	}

	function getProperty( required string key, any defaultValue ) {
		if( structKeyExists( getProperties(), key ) ) {
			return getProperties()[ key ];
		} else if( !isNull( arguments.defaultValue ) ) {
			return defaultValue;
		} else {
			return;
		}
	}

	function setProperty( required string key, required any value ) {
		getProperties()[ key ] = value;
		return this;
	}

	function routeInternal( required WebSocketSTOMP STOMPBroker, required any originalMessage, required any channel, required string destination, required string subscriptionID ) {
		if( channel.isOpen() ) {			
			var message = originalMessage.clone();

			// Setup new message object
			message.setCommand( "MESSAGE" );
			message.setHeader( "subscription", subscriptionID );
			message.setHeader( "message-id", createGUID() );
			message.setHeader( "destination", destination );
			STOMPBroker.sendMessage( STOMPBroker.getMessageParser().serialize( message ), channel );
		}
	
	}

}