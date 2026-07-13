<cfsetting enablecfoutputonly="true">
<cfsetting showdebugoutput="false">

<!--- Execute the project specs through a CFML web server such as Lucee. --->
<cfparam name="url.reporter" default="json">
<cfparam name="url.directory" default="tests.specs">
<cfparam name="url.recurse" default="true" type="boolean">
<cfparam name="url.bundles" default="">
<cfparam name="url.labels" default="">
<cfparam name="url.excludes" default="">
<cfparam name="url.reportpath" default="#expandPath( "/tests/results" )#">
<cfparam name="url.propertiesFilename" default="TEST.properties">
<cfparam name="url.propertiesSummary" default="true" type="boolean">
<cfparam name="url.bundlesPattern" default="*Spec*.cfc|*Test*.cfc">

<cfif !directoryExists( url.reportpath )>
	<cfdirectory action="create" directory="#url.reportpath#" recurse="true">
</cfif>

<cfinclude template="../testbox/system/runners/HTMLRunner.cfm">
