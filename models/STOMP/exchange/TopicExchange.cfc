/**
 * Topic STOMP exchange.  Will route messages to all subscribers of a topic, allowing wildcards
 */
component extends="BaseExchange" {

	function init( struct properties={} ) {
		arguments.properties.bindings = arguments.properties.bindings ?: {};
		// precalculate the regex
		// # matches any char for the rest of the string
		// * matches any char for the rest of the segment, ending with a literal .
		arguments.properties.bindings = arguments.properties.bindings.reduce( (bindings,k,v)=>{
			var regex = "^" & k.lcase().replace( ".", "\.", "all" ).replace( "*", "[^\.]*", "all" ).replace( "##", ".*", "all" ) & "$";
			bindings[ regex ] = v;
			return bindings;
		}, {} )
		super.init( properties=arguments.properties );
		return this;
	}

	function routeMessage( required WebSocketSTOMP STOMPBroker, required string destination, required any message ) {
		destination = destination.lcase();
		var bindings = getProperty( "bindings", {} );
		for( var regex in bindings ) {
			// both the regex and the destination are lowercased already
			if( reFind( regex, destination ) ) {
				var newDestination = bindings[ regex ];
				// Start from the top again, since we can route to another exchange
				StompBroker.routeMessage( newDestination, message );
			}
		}
	}

}