/**
 * Distribution STOMP exchange.  Will route messages to one of the bound destinations 
 */
component extends="BaseExchange" {

	function routeMessage( required WebSocketSTOMP STOMPBroker, required string destination, required any message ) {
		var bindings = getProperty( "bindings", {} );
		for( var routingKey in bindings ) {
			// both the regex and the destination are lowercased already
			if( routingKey == destination ) {
				// Send message to all bindings
				var possibleDesinations = bindings[ routingKey ];
				StompBroker.routeMessage( chooseNextDestination( possibleDesinations ), message );
			}
		}
	}

	function chooseNextDestination( required array possibleDestinations ) {
		var type = getProperty( "type", "random" );
		switch( type ) {
			case "random":
				return chooseRandomDestination( possibleDestinations );
			case "roundrobin":
				return chooseRoundRobinDestination( possibleDestinations );
			default:
				throw( message="Unknown distribution type", detail="The distribution type #type# is not supported", type="UnknownDistributionType" );
		}
	}

	function chooseRandomDestination( required array possibleDestinations ) {
		return possibleDestinations[ randRange( 1, arrayLen( possibleDestinations ) ) ];
	}

	function chooseRoundRobinDestination( required array possibleDestinations ) {
		// Lock to ensure that the current destination is updated atomically
		// Use application scope so we can maintain state even when in debug mode 
		var exchangeName = "STOMProundRobin-#getProperty( "name" )#";
		cflock( name="#exchangeName#", type="exclusive", timeout=5 ) {
			application[ exchangeName ] = application[ exchangeName ] ?: 0;
			if( application[ exchangeName ] == 0 ) {
				application[ exchangeName ] = 1;
			} else {
				application[ exchangeName ]++;
				if( application[ exchangeName ] > arrayLen( possibleDestinations ) ) {
					application[ exchangeName ] = 1;
				}
			}
			return possibleDestinations[ application[ exchangeName ] ];
		}
	}

}