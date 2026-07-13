/**
 * Regression tests for SocketBox hot-reconfiguration lifecycle management.
 * Run with either checked-in CommandBox server config and request /tests/runner.cfm.
 */
component extends="tests.resources.ApplicationScopedBase" {

function setup() {
	variables.applicationState = {};
	for( var key in [ "socketBox", "SocketBoxConfig", "socketBoxClusterManagement" ] ) {
		variables.applicationState[ key ] = {
			exists : application.keyExists( key )
		};
		if( variables.applicationState[ key ].exists ) {
			variables.applicationState[ key ].value = application[ key ];
		}
	}

	variables.serverManagersExisted = server.keyExists( "socketBoxManagers" );
	if( variables.serverManagersExisted ) {
		variables.originalServerManagers = server.socketBoxManagers;
	}

	// Bypass environment auto-detection: these specs exercise configuration only.
	application.socketBox = { serverType : "test" };
	application.delete( "SocketBoxConfig" );
	application.delete( "socketBoxClusterManagement" );
	server.socketBoxManagers = {};
}

function teardown() {
	// Stop a real replacement manager if a test reached the enabled state.
	if(
		application.keyExists( "socketBoxClusterManagement" ) &&
		application.socketBoxClusterManagement.keyExists( "clusterManager" )
	) {
		var manager = application.socketBoxClusterManagement.clusterManager;
		if( !isSimpleValue( manager ) ) {
			try {
				manager.shutdown();
			} catch( any ignored ) {
			}
		}
	}

	// Signal any manager thread started by a failed assertion to exit.
	server.socketBoxManagers = {};

	for( var key in variables.applicationState ) {
		if( variables.applicationState[ key ].exists ) {
			application[ key ] = variables.applicationState[ key ].value;
		} else {
			application.delete( key );
		}
	}

	if( variables.serverManagersExisted ) {
		server.socketBoxManagers = variables.originalServerManagers;
	} else {
		server.delete( "socketBoxManagers" );
	}
}

function newCore( required struct config ) {
	return createObject( "component", "tests.resources.ConfigurableWebSocketCore" )
		.setTestConfig( arguments.config );
}

function newClusterConfig( boolean enabled=true ) {
	return {
		cluster : {
			enable : arguments.enabled,
			name : "ws://current:8080/ws",
			peers : [],
			secretKey : "test-secret"
		}
	};
}

function newOldManager( boolean failOnShutdown=false ) {
	var shouldFailOnShutdown = arguments.failOnShutdown;
	var manager = {
		marker : createUUID(),
		shutdownCount : 0,
		peerStateReadCount : 0,
		futureStateReadCount : 0,
		wasCurrentAtShutdown : false
	};
	manager.getPeerConnections = ()=>{
		manager.peerStateReadCount++;
		return { "old-peer" : { marker : "must-not-transfer" } };
	};
	manager.getPendingConnectionFutures = ()=>{
		manager.futureStateReadCount++;
		return [ { marker : "must-not-transfer" } ];
	};
	manager.shutdown = ()=>{
		manager.shutdownCount++;
		manager.wasCurrentAtShutdown =
			application.socketBoxClusterManagement.clusterManager.marker == manager.marker;
		if( shouldFailOnShutdown ) {
			throw( type="ReconfigurationFailure", message="simulated old-manager shutdown failure" );
		}
	};
	return manager;
}

function installOldManager( required any manager ) {
	application.socketBoxClusterManagement = {
		clusterManager : arguments.manager,
		channels : {},
		managementChannels : {},
		selfChannels : {}
	};
}

function testEnabledReconfigurationShutsDownBeforeReplacementWithoutTransferringState() {
	var oldManager = newOldManager();
	installOldManager( oldManager );
	var core = newCore( newClusterConfig( true ) );
	var managerKey = core.getSocketBoxKey();
	server.socketBoxManagers[ managerKey ] = "old-manager-key";

	var config = core._configure();
	var newManager = application.socketBoxClusterManagement.clusterManager;

	$assert.isTrue( config.cluster.enable, "The replacement configuration should remain cluster-enabled." );
	$assert.isEqual( 1, oldManager.shutdownCount, "The old manager must be shut down exactly once." );
	$assert.isTrue( oldManager.wasCurrentAtShutdown, "Shutdown must happen while the old manager is still installed." );
	$assert.isEqual( 0, oldManager.peerStateReadCount, "Peer connections must not be copied to the replacement manager." );
	$assert.isEqual( 0, oldManager.futureStateReadCount, "Pending futures must not be copied to the replacement manager." );
	$assert.isEqual( 0, newManager.getPeerConnections().len(), "The replacement manager must begin without old peer state." );
	$assert.isTrue( len( server.socketBoxManagers[ managerKey ] ), "The replacement manager must register its own lifecycle key." );
}

function testEnabledReconfigurationCanDisableClustering() {
	var oldManager = newOldManager();
	installOldManager( oldManager );
	var core = newCore( newClusterConfig( false ) );

	var config = core._configure();

	$assert.isFalse( config.cluster.enable, "The new configuration should disable clustering." );
	$assert.isEqual( 1, oldManager.shutdownCount, "Disabling clustering must shut down the old manager once." );
	$assert.isTrue( oldManager.wasCurrentAtShutdown, "The old manager must remain installed until shutdown starts." );
	$assert.isFalse( application.keyExists( "socketBoxClusterManagement" ), "Disabling clustering must remove management state." );
}

function testReplacementFailureClearsConfigurationAndManagerRegistration() {
	var oldManager = newOldManager( true );
	installOldManager( oldManager );
	var core = newCore( newClusterConfig( true ) );
	var managerKey = core.getSocketBoxKey();
	server.socketBoxManagers[ managerKey ] = "old-manager-key";
	var failureRaised = false;

	try {
		core._configure();
	} catch( ReconfigurationFailure e ) {
		failureRaised = true;
	}

	$assert.isTrue( failureRaised, "A manager replacement failure should reach the caller." );
	$assert.isEqual( 1, oldManager.shutdownCount, "A failed shutdown must not be retried during the same replacement." );
	$assert.isFalse( application.keyExists( "SocketBoxConfig" ), "A failed replacement must remove its partial configuration." );
	$assert.isFalse( application.keyExists( "socketBoxClusterManagement" ), "A failed replacement must remove management state." );
	$assert.isEqual( "", server.socketBoxManagers[ managerKey ], "A failed replacement must invalidate the manager thread key." );
}

}
