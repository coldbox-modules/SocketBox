component {
	this.name = "SocketBoxRegressionTests";
	this.applicationTimeout = createTimeSpan( 0, 0, 5, 0 );

	var testsPath = getDirectoryFromPath( getCurrentTemplatePath() );
	var rootPath = getDirectoryFromPath( testsPath );

	this.mappings[ "/models" ] = rootPath & "models";
	this.mappings[ "/socketbox" ] = rootPath;
	this.mappings[ "/testbox" ] = rootPath & "testbox";
	this.mappings[ "/tests" ] = testsPath;
}
