# Current Feature: Working Telnyx ↔ OpenAI Realtime Call

## Goal
Make a phone call to existing Telnyx number and have bidirectional voice conversation with OpenAI Realtime API.

**Success Criteria**: Call Telnyx number → hear GPT voice greeting → speak to GPT → hear GPT voice response

## Phase 1: Fix Telnyx Streaming Setup

### Task 1.1: Fix Answer Request with Bidirectional RTP
**File**: `server.js` line 428-440
**Current Issue**: Missing `stream_bidirectional_mode` and codec specification

**Action**:
```javascript
// In /api/telnyx/voice webhook, replace existing answer call:
body: JSON.stringify({
    client_state: Buffer.from(callId.toString()).toString('base64'),
    stream_url: `wss://${req.get('host')}/api/telnyx/stream`,
    stream_track: 'both_tracks',
    stream_bidirectional_mode: 'rtp',
    stream_bidirectional_codec: 'PCMU'  // CRITICAL: Enable RTP return
})
```

**Test**: Webhook should receive `streaming.started` event after `call.answered`
**Log**: `console.log('Received streaming.started event:', event.event_type)`

### Task 1.2: Add Raw Body Middleware for Webhook Validation
**File**: `server.js` before line 35
**Action**:
```javascript
// Add before existing middleware
app.use('/api/telnyx', express.raw({ type: 'application/json' }));
app.use('/api/telnyx', (req, res, next) => {
    req.rawBody = req.body;
    req.body = JSON.parse(req.body);
    next();
});
```

**Test**: `req.rawBody` should contain Buffer for signature validation
**Log**: `console.log('Raw body length:', req.rawBody?.length)`

## Phase 2: Replace OpenAI Client with Raw WebSocket

### Task 2.1: Remove Old RealtimeClient Dependency
**File**: `telnyx-gateway.js` line 8, 230-233
**Action**: 
- Remove `import { RealtimeClient } from '@openai/realtime-api-beta'`
- Replace RealtimeClient with native WebSocket

### Task 2.2: Create Raw WebSocket Connection to OpenAI
**File**: `telnyx-gateway.js` `initializeGPTSession` method
**Action**:
```javascript
// Replace RealtimeClient initialization with:
const wsUrl = `wss://api.openai.com/v1/realtime?model=gpt-realtime`;
callData.gptWebSocket = new WebSocket(wsUrl, {
    headers: {
        'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
        'OpenAI-Beta': 'realtime=v1'
    }
});
```

**Test**: WebSocket should connect successfully
**Log**: `console.log('OpenAI WebSocket state:', ws.readyState)`

### Task 2.3: Implement OpenAI Event Handlers
**File**: `telnyx-gateway.js` `initializeGPTSession` method
**Action**: Add event handlers for connection, audio deltas, and errors
```javascript
callData.gptWebSocket.on('open', () => {
    console.log('✅ OpenAI Realtime WebSocket connected');
    // Send session configuration
    // Send initial greeting
    // Trigger response.create
});

callData.gptWebSocket.on('message', (data) => {
    const event = JSON.parse(data.toString());
    this.handleOpenAIEvent(callData, event);
});
```

**Test**: Should receive `session.created` event after connection
**Log**: `console.log('OpenAI event:', event.type, event)`

## Phase 3: Fix Audio Flow - Inbound (Telnyx → OpenAI)

### Task 3.1: Fix Telnyx Media Payload Processing
**File**: `telnyx-gateway.js` `processAudioPacket` method
**Current Issue**: Assumes RTP header exists, but Telnyx sends payload only

**Action**:
```javascript
processAudioPacket(callData, base64Payload) {
    // NO RTP header stripping - Telnyx gives payload only
    const pcmuBuffer = Buffer.from(base64Payload, 'base64');
    
    // Convert PCMU → PCM16 → 8kHz→24kHz → base64
    const pcm16Buffer = this.pcmuToPcm16(pcmuBuffer);
    const upsampled = this.upsampleAudio(pcm16Buffer, 8000, 24000);
    const base64Audio = Buffer.from(upsampled).toString('base64');
    
    // Append to OpenAI
    this.appendAudioToOpenAI(callData, base64Audio);
}
```

**Test**: Audio should not cause "invalid audio data" errors
**Log**: `console.log('PCMU→PCM16:', pcmuBuffer.length, '→', pcm16Buffer.byteLength)`

### Task 3.2: Implement Audio Batching and Commit Logic
**File**: `telnyx-gateway.js` new method
**Action**:
```javascript
appendAudioToOpenAI(callData, base64Audio) {
    // Batch audio to avoid "buffer too small" errors
    callData.audioBuffer = callData.audioBuffer || [];
    callData.audioBuffer.push(base64Audio);
    
    // Commit every ~100-200ms worth of audio
    if (callData.audioBuffer.length >= 8) { // ~160ms at 20ms packets
        this.commitAudioBuffer(callData);
    }
}

commitAudioBuffer(callData) {
    if (callData.audioBuffer.length === 0) return;
    
    const combinedAudio = Buffer.concat(
        callData.audioBuffer.map(b64 => Buffer.from(b64, 'base64'))
    ).toString('base64');
    
    // Send input_audio_buffer.append
    callData.gptWebSocket.send(JSON.stringify({
        type: 'input_audio_buffer.append',
        audio: combinedAudio
    }));
    
    console.log(`📤 Sent ${callData.audioBuffer.length} audio chunks to OpenAI`);
    callData.audioBuffer = [];
}
```

**Test**: Should not get "buffer too small" errors
**Log**: Track buffer sizes and commit frequency

## Phase 4: Fix Audio Flow - Outbound (OpenAI → Telnyx)

### Task 4.1: Fix OpenAI Event Listener
**File**: `telnyx-gateway.js` `handleOpenAIEvent` method
**Current Issue**: Wrong event name (`conversation.updated` vs `response.audio.delta`)

**Action**:
```javascript
handleOpenAIEvent(callData, event) {
    switch (event.type) {
        case 'response.audio.delta':
            console.log('🔊 GPT audio delta:', event.delta.length, 'bytes');
            this.streamGPTAudioToTelnyx(callData, event.delta);
            break;
            
        case 'input_audio_buffer.speech_started':
            console.log('🎤 User started speaking');
            break;
            
        case 'input_audio_buffer.speech_stopped':
            console.log('🎤 User stopped speaking - triggering response');
            // CRITICAL: Trigger GPT response
            callData.gptWebSocket.send(JSON.stringify({
                type: 'response.create'
            }));
            break;
    }
}
```

**Test**: Should see `response.audio.delta` events after `response.create`
**Log**: Track event types and audio delta sizes

### Task 4.2: Fix RTP Packet Building for Telnyx
**File**: `telnyx-gateway.js` `createRtpPacket` method
**Current Issue**: RTP timestamp calculation and packet structure

**Action**:
```javascript
createRtpPacket(pcmuPayload, callData) {
    // Track sequence and timestamp per call
    callData.rtpSeq = (callData.rtpSeq || 0) + 1;
    callData.rtpTimestamp = (callData.rtpTimestamp || 0) + 160; // 20ms * 8kHz
    
    const header = Buffer.alloc(12);
    header[0] = 0x80;  // Version 2
    header[1] = 0x00;  // PCMU payload type
    header.writeUInt16BE(callData.rtpSeq % 65536, 2);
    header.writeUInt32BE(callData.rtpTimestamp % 4294967296, 4);
    header.writeUInt32BE(0x12345678, 8); // SSRC
    
    return Buffer.concat([header, pcmuPayload]);
}
```

**Test**: RTP packets should play correctly on phone
**Log**: `console.log('RTP packet: seq=${seq}, ts=${ts}, payload=${payload.length})`

## Phase 5: Fix Response Generation Flow

### Task 5.1: Send Initial Greeting with Response Trigger
**File**: `telnyx-gateway.js` `initializeGPTSession` method
**Action**:
```javascript
// After WebSocket opens, send greeting
callData.gptWebSocket.send(JSON.stringify({
    type: 'conversation.item.create',
    item: {
        type: 'message',
        role: 'user',
        content: [{
            type: 'input_text',
            text: this.getGreeting(callData.agent)
        }]
    }
}));

// CRITICAL: Trigger response generation
callData.gptWebSocket.send(JSON.stringify({
    type: 'response.create'
}));
```

**Test**: Should hear GPT greeting immediately after call connects
**Log**: `console.log('Sent greeting and triggered response')`

## Phase 6: End-to-End Testing Protocol

### Test 6.1: Webhook Flow Validation
**Action**: Make test call and verify webhook sequence
**Expected Log Sequence**:
1. `call.initiated` event received
2. `call.answered` event received  
3. `streaming.started` event received
4. WebSocket connection established

### Test 6.2: Audio Flow Validation  
**Action**: Speak during call and verify audio processing
**Expected Log Sequence**:
1. `Connected Telnyx WebSocket`
2. `Start event: PCMU, 8000Hz`
3. `PCMU→PCM16: X → Y bytes` (continuous)
4. `Sent N audio chunks to OpenAI`
5. `User stopped speaking - triggering response`
6. `GPT audio delta: X bytes` (should appear)
7. `Sent GPT audio to Telnyx: RTP packet Y bytes`

### Test 6.3: Failure Mode Diagnostics
**Missing GPT Audio**: Check for `response.create` calls and `response.audio.delta` events
**Choppy Audio**: Verify RTP sequence numbers increment properly
**No Inbound Audio**: Check PCMU decoding and batch sizes

## Key Issues Fixed

1. **API Compatibility**: Replace `@openai/realtime-api-beta` with raw WebSocket for new `gpt-realtime` model
2. **Response Triggering**: Add `response.create` after greeting and speech stop events
3. **Audio Events**: Listen to `response.audio.delta` instead of `conversation.updated`
4. **Telnyx RTP**: Enable `stream_bidirectional_mode: 'rtp'` and proper packet building
5. **Audio Batching**: Implement proper buffering to avoid "buffer too small" errors