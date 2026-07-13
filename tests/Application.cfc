component {
	this.name = "SocketBoxRegressionTests";
	this.applicationTimeout = createTimeSpan( 0, 0, 5, 0 );
	this.mappings[ "/socketbox" ] = getDirectoryFromPath( getDirectoryFromPath( getCurrentTemplatePath() ) );
}
