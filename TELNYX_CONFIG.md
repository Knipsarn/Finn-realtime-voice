# Telnyx Configuration

## Webhook URLs
- **Primary Webhook**: https://web-production-b99cf.up.railway.app/api/telnyx/voice
- **Failover Webhook**: https://web-production-b99cf.up.railway.app/api/telnyx/voice
- **WebSocket Stream**: wss://web-production-b99cf.up.railway.app/api/telnyx/stream

## Recommended Telnyx Voice API Application Settings

### Basic Settings
- **Tags**: `gpt-realtime`, `swedish-ai`, `voice-assistant`
- **Webhook Failover URL**: https://web-production-b99cf.up.railway.app/api/telnyx/voice
- **Webhook Method**: POST (default)

### Advanced Settings
- **Enable "hang-up" on timeout**: ✅ YES (recommended)
- **Custom webhook timeout**: 10 seconds (default is fine)
- **DTMF Type**: RFC 2833 (default, works best)
- **Enable RTCP capture**: ❌ NO (not needed for this use case)
- **Enable Call cost**: ✅ YES (useful for monitoring)

### Why These Settings:
1. **Hang-up on timeout**: Prevents stuck calls if our server is down
2. **10 second timeout**: Gives our server time to respond to webhooks
3. **RFC 2833 DTMF**: Standard DTMF handling for phone interactions
4. **RTCP capture disabled**: We don't need real-time control protocol data
5. **Call cost enabled**: Good for billing/monitoring usage

## Phone Number Assignment
- Assign your Swedish phone number to this Voice API Application
- Make sure the number is configured for inbound calling

## Testing
- Test webhook: `curl -X POST https://web-production-b99cf.up.railway.app/api/telnyx/voice`
- Expected response: JSON with answer and streaming_start commands