/**
 * Direct STOMP exchange.  Will route messages directly 
 */
component extends="BaseExchange" {

	function routeMessage( required WebSocketSTOMP STOMPBroker, required any message ) {
		var destination = message.getHeader( "destination", "" );
		var subs = STOMPBroker.getSubscriptions();
		if( !structKeyExists( subs, destination ) ) {
			return;
		}
		// Change from SEND to MESSAGE
		message.setCommand( "MESSAGE" );
		// Remove sensitive details
		message.removeHeader("login");
		message.removeHeader("passcode");
		subs[ destination ].each( function( subscriptionID, subscription ) {
			var channel = subscription[ "channel" ];
			if( channel.isOpen() ) {
				message.setHeader( "subscription", subscription.subscriptionID );
				STOMPBroker.sendMessage( STOMPBroker.getMessageParser().serialize( message ), channel );
			}
		} );
	}

}