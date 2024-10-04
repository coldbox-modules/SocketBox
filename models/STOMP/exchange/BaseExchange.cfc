/**
 * Base class for STOMP exchanges
 */
component accessors="true" {
	property name="properties" type="struct";

	function init( struct properties={} ) {
		setProperties( arguments.properties );
		return this;
	}

	function routeMessage( required WebSocketSTOMP STOMPBroker, required any message ){
		throw( message="Method not implemented", detail="You must implement the routeMessage method in your exchange", type="MethodNotImplemented" );
	}

	function getProperty( required string key, string defaultValue ) {
		if( structKeyExists( getProperties(), key ) ) {
			return getProperties()[ key ];
		} else if( !isNull( arguments.defaultValue ) ) {
			return defaultValue;
		} else {
			return;
		}
	}

	function setProperty( required string key, required string value ) {
		getProperties()[ key ] = value;
		return this;
	}

}