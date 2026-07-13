component {
	this.name = "SocketBoxRegressionTests";
	this.applicationTimeout = createTimeSpan( 0, 0, 5, 0 );

	variables.testsPath = getDirectoryFromPath( getCurrentTemplatePath() );
	variables.rootPath = getDirectoryFromPath( variables.testsPath );

	this.mappings[ "/models" ] = variables.rootPath & "models";
	this.mappings[ "/socketbox" ] = variables.rootPath;
	this.mappings[ "/testbox" ] = variables.rootPath & "testbox";
	this.mappings[ "/tests" ] = variables.testsPath;
}
