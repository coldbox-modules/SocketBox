<!---
	TestBox base component that supplies the application scope used by
	WebSocketCore in both BoxLang CLI and CFML web-server runners.
--->
<cfcomponent extends="testbox.system.BaseSpec" output="false">
	<cfapplication name="SocketBoxRegressionTests">
</cfcomponent>
