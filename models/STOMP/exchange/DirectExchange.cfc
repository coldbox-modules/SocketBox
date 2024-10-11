/**
 * Direct STOMP exchange.  Will route messages directly 
 */
component extends="BaseExchange" {

	function routeMessage( required WebSocketSTOMP STOMPBroker, required string destination, required any message ) {
		var subs = STOMPBroker.getSubscriptions();
		if( structKeyExists( subs, destination ) ) {
			// Route exact matches to subscriptions
			subs[ destination ].each( function( channelSubID, subscription ) {
				if( subscription.type == "channel" ) {
					var subscriptionID = subscription.subscriptionID;
					// actually send to a channel.
					routeInternal( STOMPBroker, message, subscription[ "channel" ], destination, subscriptionID )
				} else {
					// type == "internal"
					subscription[ "callback" ]( message );
				}
			} );
		}

		var bindings = getProperty( "bindings", {} );
		if( structKeyExists( bindings, destination ) ) {
			bindings[ destination ].each( function( bindingID, binding ) {
				// This will match an exchange and process again from the top
				STOMPBroker.routeMessage( destination, message )
			} );
		}

	}

}