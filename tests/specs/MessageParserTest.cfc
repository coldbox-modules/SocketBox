/**
 * Regression tests for STOMP frame body parsing, covering the content-length
 * header being a count of octets while the frame arrives as a decoded string.
 */
component extends="testbox.system.BaseSpec" {

variables.NULL_BYTE = URLDecode( "%00" );

function setup() {
	variables.parser = new models.STOMP.MessageParser();
}

function byteLength( required string value ) {
	return arrayLen( value.getBytes( "utf-8" ) );
}

/**
 * Build a SEND frame the way a client would: content-length is the octet count of the body.
 */
function sendFrame( required string body, numeric contentLength ) {
	var frame = "SEND" & chr( 10 ) & "destination:/topic/test" & chr( 10 );
	if( !isNull( arguments.contentLength ) ) {
		frame &= "content-length:" & arguments.contentLength & chr( 10 );
	}
	return frame & chr( 10 ) & arguments.body & NULL_BYTE;
}

function assertRoundTrip( required string body, required string label ) {
	var parsed = parser.deserialize( sendFrame( body, byteLength( body ) ), nullValue() );
	$assert.isEqual( body, parsed.getBodyRaw(), "#label# body must survive parsing intact." );
}

function testAsciiBodyRoundTrips() {
	assertRoundTrip( "plain ascii body", "An ASCII" );
}

function testTwoByteCharactersRoundTrip() {
	assertRoundTrip( "café naïve über", "A two-byte accented Latin" );
}

function testThreeByteCharactersRoundTrip() {
	assertRoundTrip( "em dash —", "A three-byte punctuation" );
	assertRoundTrip( "日本語テキスト", "A three-byte CJK" );
}

function testFourByteCharactersRoundTrip() {
	assertRoundTrip( "🎉", "A four-byte surrogate pair" );
	assertRoundTrip( "🎉 party 🎊 time 🥳", "A multiple surrogate pair" );
}

function testMixedWidthCharactersRoundTrip() {
	assertRoundTrip( "café — 日本語 🎉 ascii", "A mixed-width" );
}

function testJSONBodyWithMultiByteCharacterRoundTrips() {
	assertRoundTrip( '{"text":"em dash — probe"}', "A JSON" );
}

function testNullTerminatedBodyRoundTripsWithoutContentLength() {
	var body = "café — 日本語 🎉 ascii";
	var parsed = parser.deserialize( sendFrame( body ), nullValue() );
	$assert.isEqual( body, parsed.getBodyRaw(), "A body read to the null byte must survive parsing intact." );
}

function testTruncatedBodyStillThrows() {
	var body = "café — 日本語 🎉 ascii";
	// Claim ten more octets than were actually sent.
	var frame = sendFrame( body, byteLength( body ) + 10 );
	$assert.throws( () => parser.deserialize( frame, nullValue() ), "", "content-length header specified" );
}

function testBodyMissingNullByteStillThrows() {
	var body = "café — 日本語 🎉 ascii";
	var frame = "SEND" & chr( 10 ) & "destination:/topic/test" & chr( 10 )
		& "content-length:" & byteLength( body ) & chr( 10 ) & chr( 10 ) & body;
	$assert.throws( () => parser.deserialize( frame, nullValue() ), "", "missing null byte" );
}

function testSerializedContentLengthIsAnOctetCount() {
	var body = "café — 日本語 🎉 ascii";
	var message = new models.STOMP.Message( "SEND", { "destination" : "/topic/test" }, body );
	parser.serialize( message );
	$assert.isEqual( byteLength( body ), message.getHeader( "content-length" ), "content-length must be the octet count, not the character count." );
}

function testSerializeDeserializeRoundTripsMultiByteBody() {
	var body = "café — 日本語 🎉 ascii";
	var message = new models.STOMP.Message( "SEND", { "destination" : "/topic/test" }, body );
	var parsed = parser.deserialize( parser.serialize( message ), nullValue() );
	$assert.isEqual( body, parsed.getBodyRaw(), "A frame this server emitted must parse back to the same body." );
}

}
