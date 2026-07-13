/**
 * Test fixture that makes WebSocketCore configuration deterministic and quiet.
 */
component extends="models.WebSocketCore" {

	variables.testConfig = {};

	function setTestConfig( required struct config ) {
		variables.testConfig = arguments.config;
		return this;
	}

	function configure() {
		return variables.testConfig;
	}

	function logMessage( required any message ) {
	}

	function println( required message ) {
	}

}
