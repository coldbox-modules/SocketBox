/**
 * I represent a connection made from us to another peer in our cluster.
 */
component extends="cbproxies.models.BaseProxy" accessors="true" {
	property name="webSocket" type="any";
	property name="socketBox" type="any";
	property name="clusterManager" type="any";
	property name="peerName" type="any";
	property name="websocketHash" type="any";

	/**
	 * Constructor
	 * @socketBox The SocketBox instance to use for cluster management
	 * @clusterManager The ClusterManager instance managing this peer
	 * @peerName The name of this peer
	 */
	function init( required any socketBox, required any clusterManager, required string peerName ) {
		super.init( "" );
		variables.socketBox = socketBox;
		variables.clusterManager = clusterManager;
		variables.peerName = peerName;
		variables.webSocketHash = "";
		return this;
	}

	/**
	 * Called when the websocket connection is opened
	 * @param webSocket The WebSocket instance
	 */
    public void function onOpen(required any webSocket) {
		execute(
			()=>{
				setWebSocket( webSocket );
				setWebsocketHash( webSocket.hashCode() );
				// Must request messages to start receiving them
				webSocket.request(1);
			},
			"ManagementListener",
			arguments
		);
    }
    
	/**
	 * Called when a text message is received
	 * @param webSocket The WebSocket instance
	 * @param data The text message received
	 * @param last Indicates if this is the last part of a message
	 */
    public any function onText(required any webSocket, required any data, required boolean last) {
		execute(
			()=>{
				var message = data.toString();
				
				// Request next message
				webSocket.request(1);
				
				// Return completed CompletableFuture
				return createObject("java", "java.util.concurrent.CompletableFuture").completedFuture(javaCast("null", ""));
			},
			"ManagementListener",
			arguments
		);
    }
    
	/**
	 * Called when binary data is received
	 * @param webSocket The WebSocket instance
	 * @param data The binary data received
	 * @param last Indicates if this is the last part of a message
	 */
    public any function onBinary(required any webSocket, required any data, required boolean last) {
		execute(
			()=>{
				var size = data.remaining();
				
				// Request next message
				webSocket.request(1);
				
        		return createObject("java", "java.util.concurrent.CompletableFuture").completedFuture(javaCast("null", ""));
			},
			"ManagementListener",
			arguments
		);
    }
    
	/**
	 * Called when the WebSocket connection is closed
	 * @param webSocket The WebSocket instance
	 * @param statusCode The status code of the closure
	 * @param reason The reason for the closure
	 * @return A CompletableFuture indicating the completion of the close operation
	 */
    public any function onClose(required any webSocket, required numeric statusCode, required string reason) {
		execute(
			()=>{
				getClusterManager().removePeerConnection( this.getPeerName(), false );
				return createObject("java", "java.util.concurrent.CompletableFuture").completedFuture(javaCast("null", ""));
			},
			"ManagementListener",
			arguments
		);
    }
    
	/**
	 * Called when an error occurs on the WebSocket connection
	 * @param webSocket The WebSocket instance
	 * @param error The error that occurred
	 */
    public void function onError(required any webSocket, required any error) {
		execute(
			()=>{
				socketBox.logMessage("WebSocket error: " & error.getMessage());
			},
			"ManagementListener",
			arguments
		);
    }

    /**
	 * Send a text message to the peer
	 * @param message The message to send
	 * @return A CompletableFuture indicating the completion of the send operation
	 */
    public function sendText(required string message) {
		if( isNull( variables.webSocket ) ) {
			return;
		}
		lock name="websocket_#getWebsocketHash()#" timeout=60 type="exclusive" {
			var future = variables.webSocket.sendText(arguments.message, true);
			future.get()
		}
    }
    
	/**
	 * Send binary data to the peer
	 * @param data The binary data to send
	 * @return A CompletableFuture indicating the completion of the send operation
	 */
    public function sendBinary(required any data) {
		if( isNull( variables.webSocket ) ) {
			return;
		}
		lock name="websocket_#getWebsocketHash()#" timeout=60 type="exclusive" {
			variables.webSocket.sendBinary(arguments.data, true);
		}
    }

    /**
	 * Close the peer connection
	 * @return A CompletableFuture indicating the completion of the close operation
	 */
    public function close() {
		if( isNull( variables.webSocket ) ) {
			return;
		}
		lock name="websocket_#getWebsocketHash()#" timeout=60 type="exclusive" {
			var future = variables.webSocket.sendClose(1000, "SocketBox Peer [#clusterManager.getMyPeerName()#] shutting down");
			future.get();
		}
    }
 		
	/**
	 * Check if the peer connection is open
	 * @return true if the connection is open, false otherwise
	 */
	function isConnectionOpen() {
		try {
			return !isNull( webSocket ) && 
				!webSocket.isInputClosed() && 
				!webSocket.isOutputClosed();
		} catch( any e ) {
			socketBox.logMessage("Error checking peer connection status: " & e.message);
			return false;
		}
	}

}