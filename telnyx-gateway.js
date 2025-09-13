/**
 * Telnyx Media Streams to GPT-Realtime WebRTC Gateway
 * Handles bidirectional audio streaming and transcoding from RTP/PCMU to PCM16
 */

import { WebSocketServer } from 'ws';
import WebSocket from 'ws';
import database from './database.js';

class TelnyxGPTGateway {
    constructor(server) {
        this.server = server;
        this.activeCalls = new Map(); // callId -> { ws, gptWebSocket, agent }
        
        this.initializeWebSocketServer();
    }

    initializeWebSocketServer() {
        this.wss = new WebSocketServer({ 
            server: this.server,
            path: '/api/telnyx/stream'
        });

        this.wss.on('connection', (ws, req) => {
            console.log('=== TELNYX WEBSOCKET CONNECTION SUCCESSFUL ===');
            console.log('WebSocket connected from:', req.socket.remoteAddress);
            console.log('Protocol:', ws.protocol);
            console.log('Headers:', JSON.stringify(req.headers, null, 2));
            this.handleTelnyxConnection(ws, req);
        });
        
        this.wss.on('error', (error) => {
            console.error('=== WEBSOCKET SERVER ERROR ===');
            console.error('Error:', error);
        });
        
        this.wss.on('headers', (headers, req) => {
            console.log('=== WEBSOCKET HEADERS EVENT ===');
            console.log('Headers being sent:', headers);
            console.log('Request URL:', req.url);
        });
        
        this.wss.on('error', (error) => {
            console.error('Telnyx WebSocket Server Error:', error);
        });

        console.log('Telnyx WebSocket server initialized on /api/telnyx/stream');
    }

    async handleTelnyxConnection(ws, req) {
        let callData = {
            ws: ws,
            callControlId: null,
            streamId: null,
            agentId: null,
            agent: null,
            gptWebSocket: null,
            gptConnecting: true, // Set to true immediately to buffer early audio
            audioBuffer: [], // Buffer audio packets during connection
            startTime: Date.now(),
            // RTP state for outbound audio
            rtpSeq: Math.floor(Math.random() * 65536),
            rtpTimestamp: Math.floor(Math.random() * 4294967296)
        };

        ws.on('message', async (data) => {
            try {
                const message = JSON.parse(data.toString());
                await this.handleTelnyxMessage(ws, message, callData);
            } catch (error) {
                console.error('Error handling Telnyx message:', error);
            }
        });

        ws.on('close', () => {
            console.log(`Telnyx stream closed for call ${callData.callControlId}`);
            this.cleanup(callData);
        });

        ws.on('error', (error) => {
            console.error('Telnyx WebSocket error:', error);
            this.cleanup(callData);
        });
    }

    async handleTelnyxMessage(ws, message, callData) {
        try {
            switch (message.event) {
                case 'connected':
                    console.log('Telnyx stream connected, version:', message.version);
                    break;

                case 'start':
                    callData.streamId = message.stream_id;
                    console.log(`=== STREAM INITIALIZATION ===`);
                    console.log(`Stream ID: ${callData.streamId}`);
                    console.log('Media format:', message.start.media_format);
                    
                    // CRITICAL: Log the codec Telnyx expects
                    const expectedCodec = message.start.media_format?.encoding;
                    console.log(`🎯 TELNYX EXPECTS CODEC: ${expectedCodec}`);
                    console.log(`🔧 WE ARE USING: PCMA (payload type 8)`);
                    if (expectedCodec && expectedCodec !== 'PCMA') {
                        console.log(`⚠️  CODEC MISMATCH DETECTED! Telnyx wants ${expectedCodec}, we send PCMA`);
                    } else if (expectedCodec === 'PCMA') {
                        console.log(`✅ CODEC MATCH! Both using PCMA (A-law)`);
                    }
                    
                    // Get default agent for now - in production you'd pass this via URL params
                    const agents = await database.getAllAgents();
                    callData.agent = agents[0];
                    
                    if (!callData.agent) {
                        throw new Error('No agents configured in database');
                    }
                    
                    console.log(`Using agent ${callData.agent.id}: voice="${callData.agent.voice}"`);
                    console.log('Agent prompt preview:', callData.agent.prompt.substring(0, 100) + '...');
                    
                    // Initialize GPT-Realtime session with error handling
                    try {
                        console.log(`Starting GPT connection, buffering audio packets...`);
                        
                        await this.initializeGPTSession(callData);
                        
                        callData.gptConnecting = false;
                        this.activeCalls.set(callData.streamId, callData);
                        
                        // Process buffered audio packets
                        console.log(`Processing ${callData.audioBuffer.length} buffered audio packets`);
                        for (const audioPayload of callData.audioBuffer) {
                            try {
                                this.processAudioPacket(callData, audioPayload);
                            } catch (bufferError) {
                                console.error('Error processing buffered audio:', bufferError.message);
                            }
                        }
                        callData.audioBuffer = []; // Clear buffer
                        
                        console.log(`=== STREAM ${callData.streamId} READY FOR REAL-TIME AUDIO ===`);
                    } catch (gptError) {
                        console.error('=== GPT INITIALIZATION FAILED ===');
                        console.error('Stream ID:', callData.streamId);
                        console.error('GPT Error:', gptError.message);
                        
                        callData.gptConnecting = false;
                        callData.audioBuffer = []; // Clear buffer on failure
                        
                        // Send error response to Telnyx if possible
                        ws.send(JSON.stringify({
                            event: 'stop',
                            stream_id: callData.streamId
                        }));
                        
                        throw gptError;
                    }
                    break;

            case 'media':
                // CRITICAL: Handle audio during connection establishment
                if (!callData.gptWebSocket) {
                    if (callData.gptConnecting) {
                        // Buffer audio during connection
                        callData.audioBuffer.push(message.media.payload);
                        if (callData.audioBuffer.length > 100) { // Limit buffer size
                            callData.audioBuffer.shift(); // Remove oldest
                        }
                        return;
                    } else {
                        console.error('=== AUDIO PROCESSING ERROR: No GPT WebSocket and not connecting ===');
                        console.error('Stream ID:', callData.streamId);
                        return;
                    }
                }
                
                if (callData.gptWebSocket.readyState !== WebSocket.OPEN) {
                    if (callData.gptConnecting) {
                        // Still connecting - buffer audio
                        callData.audioBuffer.push(message.media.payload);
                        if (callData.audioBuffer.length > 100) {
                            callData.audioBuffer.shift();
                        }
                        return;
                    } else {
                        console.error('=== AUDIO PROCESSING ERROR: GPT WebSocket not open ===');
                        console.error('Stream ID:', callData.streamId);
                        console.error('Connection state:', callData.gptWebSocket.readyState);
                        return;
                    }
                }
                
                try {
                    // Process current audio packet
                    this.processAudioPacket(callData, message.media.payload);
                } catch (error) {
                    console.error('=== AUDIO PROCESSING ERROR ===');
                    console.error('Stream ID:', callData.streamId);
                    console.error('Error message:', error.message);
                    console.error('GPT connection state:', callData.gptWebSocket ? callData.gptWebSocket.readyState : 'null');
                }
                break;

            case 'stop':
                console.log(`=== STREAM ${callData.streamId} TERMINATED ===`);
                console.log('Termination source: Telnyx');
                await this.endCall(callData);
                break;
                
            default:
                console.log(`Unknown Telnyx event: ${message.event}`);
                break;
        }
        } catch (error) {
            console.error('=== TELNYX MESSAGE HANDLING ERROR ===');
            console.error('Stream ID:', callData.streamId);
            console.error('Message event:', message.event);
            console.error('Error message:', error.message);
            console.error('Error stack:', error.stack);
            
            // Attempt cleanup on critical errors
            if (error.message.includes('GPT') || error.message.includes('agents')) {
                this.cleanup(callData);
            }
        }
    }

    async initializeGPTSession(callData) {
        try {
            // CRITICAL: Validate API key exists before connection attempt
            if (!process.env.OPENAI_API_KEY) {
                throw new Error('OPENAI_API_KEY environment variable not set');
            }
            
            if (process.env.OPENAI_API_KEY.length < 20) {
                throw new Error('OPENAI_API_KEY appears invalid (too short)');
            }

            console.log('Initializing GPT session with API key:', process.env.OPENAI_API_KEY.substring(0, 7) + '...');

            // Create raw WebSocket connection to new gpt-realtime API
            const wsUrl = `wss://api.openai.com/v1/realtime?model=gpt-realtime`;
            callData.gptWebSocket = new WebSocket(wsUrl, {
                headers: {
                    'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
                    'OpenAI-Beta': 'realtime=v1'
                }
            });

            console.log('OpenAI WebSocket created, URL:', wsUrl);

            // Set up WebSocket event handlers
            callData.gptWebSocket.on('open', () => {
                console.log('✅ OpenAI Realtime WebSocket connected');
                console.log('Stream ID:', callData.streamId);
                console.log('Agent voice:', callData.agent.voice);
                console.log('WebSocket state:', callData.gptWebSocket.readyState);
                
                // Send session configuration
                callData.gptWebSocket.send(JSON.stringify({
                    type: 'session.update',
                    session: {
                        voice: callData.agent.voice,
                        instructions: callData.agent.prompt,
                        input_audio_format: 'pcm16',
                        output_audio_format: 'pcm16',
                        input_audio_transcription: { model: 'whisper-1' },
                        turn_detection: {
                            type: 'server_vad',
                            threshold: 0.5,
                            prefix_padding_ms: 300,
                            silence_duration_ms: 500
                        },
                        tools: [],
                        tool_choice: 'auto',
                        temperature: 0.8,
                        max_response_output_tokens: 4096
                    }
                }));
                
                // Send initial greeting
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
                
                // Trigger response generation
                callData.gptWebSocket.send(JSON.stringify({
                    type: 'response.create'
                }));
                
                console.log('📤 Sent session config, greeting, and response trigger');
            });

            callData.gptWebSocket.on('close', () => {
                console.log('=== GPT-Realtime WebSocket CLOSED ===');
                console.log('Stream ID:', callData.streamId);
            });

            callData.gptWebSocket.on('error', (error) => {
                console.error('=== GPT-Realtime WebSocket ERROR ===');
                console.error('Stream ID:', callData.streamId);
                console.error('Error details:', error.message);
            });

            callData.gptWebSocket.on('message', (data) => {
                try {
                    const event = JSON.parse(data.toString());
                    console.log('OpenAI event:', event.type);
                    // Handle events (will be implemented in Task 2.3)
                    this.handleOpenAIEvent(callData, event);
                } catch (error) {
                    console.error('Error parsing OpenAI message:', error);
                }
            });

            // WebSocket will connect automatically after event handlers are set
            console.log('Waiting for WebSocket connection...');
            
            // Wait for connection with timeout
            await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('OpenAI WebSocket connection timeout after 10 seconds'));
                }, 10000);
                
                callData.gptWebSocket.on('open', () => {
                    clearTimeout(timeout);
                    console.log('✅ OpenAI WebSocket connection established');
                    resolve();
                });
                
                callData.gptWebSocket.on('error', (error) => {
                    clearTimeout(timeout);
                    reject(error);
                });
            });

            console.log('GPT-Realtime WebSocket session initialized for stream', callData.streamId);

        } catch (error) {
            console.error('=== CRITICAL: GPT SESSION INITIALIZATION FAILED ===');
            console.error('Stream ID:', callData.streamId);
            console.error('Error message:', error.message);
            console.error('Error stack:', error.stack);
            
            // Clean up failed WebSocket
            if (callData.gptWebSocket) {
                try {
                    callData.gptWebSocket.close();
                } catch (disconnectError) {
                    console.error('Error during cleanup close:', disconnectError);
                }
                callData.gptWebSocket = null;
            }
            
            throw error; // Re-throw to prevent silent failure
        }
    }

    processAudioPacket(callData, audioPayload) {
        // Transcode and forward audio to GPT-Realtime via WebSocket
        const audioData = this.transcodeToGPT(audioPayload);
        if (audioData && callData.gptWebSocket && callData.gptWebSocket.readyState === WebSocket.OPEN) {
            // Send audio via WebSocket (will be implemented in Task 3.2)
            this.appendAudioToOpenAI(callData, audioData);
            
            // Debug audio flow (log 1% of packets to avoid spam)
            if (Math.random() < 0.01) {
                console.log(`Audio forwarded to GPT via WebSocket: ${audioData.length} bytes`);
            }
        }
    }

    // Placeholder for Task 3.2 - will implement proper audio batching
    appendAudioToOpenAI(callData, base64Audio) {
        // TODO: Implement in Task 3.2
        console.log('TODO: appendAudioToOpenAI - will batch and send audio to OpenAI WebSocket');
    }

    // OpenAI Realtime event handler
    handleOpenAIEvent(callData, event) {
        switch (event.type) {
            case 'session.created':
                console.log('🎯 OpenAI session created:', event.session.id);
                break;
                
            case 'session.updated':
                console.log('🔧 OpenAI session updated');
                break;
                
            case 'response.audio.delta':
                console.log('🔊 GPT audio delta:', event.delta.length, 'bytes');
                this.streamGPTAudioToTelnyx(callData, event.delta);
                break;
                
            case 'response.audio.done':
                console.log('✅ GPT audio response completed');
                break;
                
            case 'input_audio_buffer.speech_started':
                console.log('🎤 User started speaking');
                break;
                
            case 'input_audio_buffer.speech_stopped':
                console.log('🎤 User stopped speaking - triggering response');
                // Trigger GPT response after user stops speaking
                callData.gptWebSocket.send(JSON.stringify({
                    type: 'response.create'
                }));
                break;
                
            case 'response.done':
                console.log('🏁 GPT response completed');
                break;
                
            case 'error':
                console.error('❌ OpenAI Realtime error:', event.error);
                break;
                
            default:
                console.log('📨 OpenAI event:', event.type);
                break;
        }
    }

    transcodeToGPT(base64RtpAudio) {
        try {
            // Decode base64 to buffer
            const rtpBuffer = Buffer.from(base64RtpAudio, 'base64');
            
            // Extract RTP payload (remove RTP header - first 12 bytes)
            const rtpHeaderSize = 12;
            const payloadBuffer = rtpBuffer.slice(rtpHeaderSize);
            
            // Decode PCMU to 16-bit PCM
            const pcmBuffer = this.pcmuToPcm16(payloadBuffer);
            
            // Upsample from 8kHz to 24kHz for OpenAI
            const upsampledBuffer = this.upsampleAudio(pcmBuffer, 8000, 24000);
            
            return Buffer.from(upsampledBuffer).toString('base64');
        } catch (error) {
            console.error('Audio transcoding error:', error);
            return null;
        }
    }

    pcmuToPcm16(pcmuBuffer) {
        // PCMU (μ-law) to linear PCM conversion
        const BIAS = 0x84;
        const CLIP = 8159;
        
        const pcmSamples = new Int16Array(pcmuBuffer.length);
        
        for (let i = 0; i < pcmuBuffer.length; i++) {
            const ulawByte = pcmuBuffer[i];
            const sign = (ulawByte & 0x80) ? -1 : 1;
            const exponent = (ulawByte >> 4) & 0x07;
            const mantissa = ulawByte & 0x0F;
            
            let sample = ((mantissa << (exponent + 3)) + BIAS - 4) * sign;
            sample = Math.max(-CLIP, Math.min(CLIP, sample));
            pcmSamples[i] = sample;
        }
        
        return pcmSamples.buffer;
    }

    upsampleAudio(pcm16Buffer, fromRate, toRate) {
        const ratio = toRate / fromRate;
        const inputSamples = new Int16Array(pcm16Buffer);
        const outputLength = Math.floor(inputSamples.length * ratio);
        const outputSamples = new Int16Array(outputLength);
        
        for (let i = 0; i < outputLength; i++) {
            const srcIndex = i / ratio;
            const index = Math.floor(srcIndex);
            const fraction = srcIndex - index;
            
            if (index + 1 < inputSamples.length) {
                // Linear interpolation
                outputSamples[i] = Math.round(
                    inputSamples[index] * (1 - fraction) + 
                    inputSamples[index + 1] * fraction
                );
            } else {
                outputSamples[i] = inputSamples[index] || 0;
            }
        }
        
        return outputSamples.buffer;
    }

    downsampleAudio(pcm16Buffer, fromRate, toRate) {
        const ratio = fromRate / toRate; // 24000/8000 = 3
        const inputSamples = new Int16Array(pcm16Buffer);
        const outputLength = Math.floor(inputSamples.length / ratio);
        const outputSamples = new Int16Array(outputLength);
        
        console.log(`Downsampling: ${inputSamples.length} samples → ${outputLength} samples (ratio: ${ratio})`);
        
        for (let i = 0; i < outputLength; i++) {
            const srcIndex = Math.floor(i * ratio);
            outputSamples[i] = inputSamples[srcIndex] || 0;
        }
        
        // Return properly sized buffer, not the full ArrayBuffer
        const resultBuffer = Buffer.allocUnsafe(outputLength * 2); // 2 bytes per Int16
        for (let i = 0; i < outputLength; i++) {
            resultBuffer.writeInt16LE(outputSamples[i], i * 2);
        }
        
        console.log(`Downsampling result: ${resultBuffer.length} bytes`);
        return resultBuffer.buffer;
    }

    pcm16ToPcmu(pcm16Buffer) {
        // μ-law (PCMU) encoding
        const pcmSamples = new Int16Array(pcm16Buffer);
        const pcmuBuffer = Buffer.alloc(pcmSamples.length);
        
        for (let i = 0; i < pcmSamples.length; i++) {
            let sample = Math.max(-32768, Math.min(32767, pcmSamples[i]));
            
            // μ-law compression
            let sign = 0;
            if (sample < 0) {
                sample = -sample;
                sign = 0x80;
            }
            
            sample += 0x84; // Bias
            if (sample > 0x7FFF) sample = 0x7FFF;
            
            let exponent = 7;
            for (let exp = 0; exp < 8; exp++) {
                if (sample <= (0x1F << (exp + 3))) {
                    exponent = exp;
                    break;
                }
            }
            
            let mantissa = (sample >> (exponent + 3)) & 0x0F;
            pcmuBuffer[i] = ~(sign | (exponent << 4) | mantissa);
        }
        
        return pcmuBuffer;
    }

    pcm16ToPcma(pcm16Buffer) {
        // A-law encoding for PCMA
        const pcmSamples = new Int16Array(pcm16Buffer);
        const pcmaBuffer = Buffer.alloc(pcmSamples.length);
        
        for (let i = 0; i < pcmSamples.length; i++) {
            let sample = pcmSamples[i];
            let sign = 0x00;
            
            if (sample < 0) {
                sample = -sample;
                sign = 0x80;
            }
            
            // Clip to maximum value
            if (sample > 32635) sample = 32635;
            
            let exponent = 7;
            let mantissa = 0;
            
            // Find exponent and mantissa for A-law
            if (sample >= 256) {
                for (exponent = 0; exponent < 7; exponent++) {
                    if (sample <= (256 << exponent)) break;
                }
                mantissa = (sample >> (exponent + 4)) & 0x0F;
            } else {
                exponent = 0;
                mantissa = sample >> 4;
            }
            
            // A-law has inverted exponent bits
            exponent ^= 0x07;
            
            pcmaBuffer[i] = sign | (exponent << 4) | mantissa;
        }
        
        return pcmaBuffer;
    }

    createRtpPacket(pcmaPayload, callData) {
        // Track sequence and timestamp per call  
        callData.rtpSeq = (callData.rtpSeq + 1) % 65536;
        // Increment timestamp by actual sample count (each PCMA byte = 1 sample at 8kHz)
        callData.rtpTimestamp = (callData.rtpTimestamp + pcmaPayload.length) % 4294967296;
        
        const header = Buffer.alloc(12);
        header[0] = 0x80;  // Version 2
        header[1] = 0x08;  // PCMA payload type (A-law)
        header.writeUInt16BE(callData.rtpSeq, 2);
        header.writeUInt32BE(callData.rtpTimestamp, 4);
        header.writeUInt32BE(0x12345678, 8); // SSRC
        
        console.log(`RTP packet: seq=${callData.rtpSeq}, ts=${callData.rtpTimestamp}, payload=${pcmaPayload.length}bytes`);
        
        return Buffer.concat([header, pcmaPayload]);
    }

    streamGPTAudioToTelnyx(callData, audioDelta) {
        try {
            // audioDelta is base64-encoded PCM16 audio from GPT (24kHz, 1 channel)
            if (!audioDelta || !callData.ws) {
                console.error('Missing audio data or WebSocket connection');
                return;
            }
            
            console.log('Processing GPT audio chunk, base64 length:', audioDelta.length);
            
            // Decode GPT's PCM16 audio from base64
            const pcm16Buffer = Buffer.from(audioDelta, 'base64');
            console.log('Decoded PCM16 buffer size:', pcm16Buffer.length, 'bytes');
            
            // Downsample from 24kHz to 8kHz for Telnyx
            const downsampledBuffer = this.downsampleAudio(pcm16Buffer, 24000, 8000);
            console.log('Downsampled buffer size:', downsampledBuffer.byteLength, 'bytes');
            
            // Convert PCM16 to PCMA (A-law) - matching Telnyx expectation
            const pcmaBuffer = this.pcm16ToPcma(downsampledBuffer);
            console.log('PCMA buffer size:', pcmaBuffer.length, 'bytes');
            
            // CRITICAL: Split into 20ms packets (160 bytes each at 8kHz)
            const packetSize = 160; // 20ms at 8kHz = 160 samples = 160 PCMA bytes
            let packetCount = 0;
            
            for (let offset = 0; offset < pcmaBuffer.length; offset += packetSize) {
                const packetPayload = pcmaBuffer.slice(offset, Math.min(offset + packetSize, pcmaBuffer.length));
                
                // Create RTP packet for this 20ms chunk
                const rtpPacket = this.createRtpPacket(packetPayload, callData);
                
                // Send to Telnyx WebSocket
                const message = {
                    event: 'media',
                    stream_id: callData.streamId,
                    media: {
                        payload: rtpPacket.toString('base64')
                    }
                };
                
                callData.ws.send(JSON.stringify(message));
                packetCount++;
            }
            
            console.log(`✅ Sent ${packetCount} RTP packets (${pcmaBuffer.length} PCMA bytes total) to Telnyx`);
            
        } catch (error) {
            console.error('=== GPT AUDIO STREAMING ERROR ===');
            console.error('Error details:', error.message);
            console.error('Stack trace:', error.stack);
            console.error('Audio delta length:', audioDelta?.length || 'undefined');
            console.error('WebSocket state:', callData.ws?.readyState || 'undefined');
        }
    }


    getGreeting(agent) {
        // Determine greeting based on agent locale/language
        if (agent.prompt.includes('svenska') || agent.prompt.includes('Swedish')) {
            return 'Hej! Vad kan jag hjälpa dig med idag?';
        }
        return 'Hello! How can I help you today?';
    }

    async endCall(callData) {
        if (callData.streamId) {
            const duration = Date.now() - callData.startTime;
            console.log(`Stream ${callData.streamId} ended, duration: ${duration}ms`);
        }
        
        this.cleanup(callData);
    }

    cleanup(callData) {
        if (callData.gptWebSocket) {
            try {
                callData.gptWebSocket.close();
            } catch (error) {
                console.error('Error closing GPT WebSocket:', error);
            }
        }
        if (callData.streamId) {
            this.activeCalls.delete(callData.streamId);
        }
    }
}

export default TelnyxGPTGateway;