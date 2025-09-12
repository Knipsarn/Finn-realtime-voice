/**
 * Telnyx Media Streams to GPT-Realtime WebRTC Gateway
 * Handles bidirectional audio streaming and transcoding from RTP/PCMU to PCM16
 */

import { WebSocketServer } from 'ws';
import OpenAI from 'openai';
import { RealtimeClient } from '@openai/realtime-api-beta';
import database from './database.js';

class TelnyxGPTGateway {
    constructor(server) {
        this.server = server;
        this.openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
        this.activeCalls = new Map(); // callId -> { ws, gptConnection, agent }
        
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
            gptClient: null,
            gptConnecting: false,
            audioBuffer: [], // Buffer audio packets during connection
            startTime: Date.now()
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
                        callData.gptConnecting = true;
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
                if (!callData.gptClient) {
                    if (callData.gptConnecting) {
                        // Buffer audio during connection
                        callData.audioBuffer.push(message.media.payload);
                        if (callData.audioBuffer.length > 100) { // Limit buffer size
                            callData.audioBuffer.shift(); // Remove oldest
                        }
                        return;
                    } else {
                        console.error('=== AUDIO PROCESSING ERROR: No GPT client and not connecting ===');
                        console.error('Stream ID:', callData.streamId);
                        return;
                    }
                }
                
                if (!callData.gptClient.isConnected()) {
                    if (callData.gptConnecting) {
                        // Still connecting - buffer audio
                        callData.audioBuffer.push(message.media.payload);
                        if (callData.audioBuffer.length > 100) {
                            callData.audioBuffer.shift();
                        }
                        return;
                    } else {
                        console.error('=== AUDIO PROCESSING ERROR: GPT client not connected ===');
                        console.error('Stream ID:', callData.streamId);
                        console.error('Connection state:', callData.gptClient.isConnected());
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
                    console.error('GPT connection state:', callData.gptClient ? callData.gptClient.isConnected() : 'null');
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

            // Initialize RealtimeClient from beta library
            callData.gptClient = new RealtimeClient({
                apiKey: process.env.OPENAI_API_KEY,
                dangerouslyAllowAPIKeyInBrowser: false
            });

            console.log('RealtimeClient created, connection state:', callData.gptClient.isConnected());

            // Set up connection event handlers BEFORE connecting
            callData.gptClient.on('connected', () => {
                console.log('=== GPT-Realtime CONNECTION ESTABLISHED ===');
                console.log('Stream ID:', callData.streamId);
                console.log('Agent voice:', callData.agent.voice);
                console.log('Connection state verified:', callData.gptClient.isConnected());
            });

            callData.gptClient.on('disconnected', () => {
                console.log('=== GPT-Realtime CONNECTION LOST ===');
                console.log('Stream ID:', callData.streamId);
            });

            callData.gptClient.on('error', (error) => {
                console.error('=== GPT-Realtime CONNECTION ERROR ===');
                console.error('Stream ID:', callData.streamId);
                console.error('Error details:', error);
                console.error('Stack trace:', error.stack);
            });

            // Configure session BEFORE connecting
            callData.gptClient.updateSession({
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
            });

            // Set up conversation event handlers
            callData.gptClient.on('conversation.updated', (event) => {
                console.log('Conversation updated:', event.type);
            });

            callData.gptClient.on('conversation.item.appended', (event) => {
                if (event.item.type === 'message' && event.item.role === 'assistant') {
                    console.log('GPT response received');
                }
            });

            callData.gptClient.on('conversation.item.completed', (event) => {
                if (event.item.type === 'message' && event.item.role === 'assistant' && event.item.content) {
                    // Handle audio content
                    for (const content of event.item.content) {
                        if (content.type === 'audio') {
                            console.log('Streaming GPT audio response to Telnyx');
                            this.streamGPTAudioToTelnyx(callData, content.audio);
                        }
                    }
                }
            });

            console.log('Pre-connection state:', callData.gptClient.isConnected());
            
            // CRITICAL: Connect to OpenAI with explicit verification
            await callData.gptClient.connect();
            
            console.log('Post-connection state:', callData.gptClient.isConnected());
            
            // CRITICAL: Verify connection actually established
            if (!callData.gptClient.isConnected()) {
                throw new Error('RealtimeClient.connect() returned success but connection not established');
            }

            // Send initial greeting only after verified connection
            callData.gptClient.sendUserMessageContent([{
                type: 'input_text',
                text: this.getGreeting(callData.agent)
            }]);

            console.log('GPT-Realtime session fully initialized for stream', callData.streamId);

        } catch (error) {
            console.error('=== CRITICAL: GPT SESSION INITIALIZATION FAILED ===');
            console.error('Stream ID:', callData.streamId);
            console.error('Error message:', error.message);
            console.error('Error stack:', error.stack);
            
            // Clean up failed client
            if (callData.gptClient) {
                try {
                    callData.gptClient.disconnect();
                } catch (disconnectError) {
                    console.error('Error during cleanup disconnect:', disconnectError);
                }
                callData.gptClient = null;
            }
            
            throw error; // Re-throw to prevent silent failure
        }
    }

    processAudioPacket(callData, audioPayload) {
        // Transcode and forward audio to GPT-Realtime
        const audioData = this.transcodeToGPT(audioPayload);
        if (audioData) {
            // Convert base64 to Int16Array as required by RealtimeClient
            const buffer = Buffer.from(audioData, 'base64');
            const int16Array = new Int16Array(buffer.buffer, buffer.byteOffset, buffer.length / 2);
            
            // Append audio to GPT client
            callData.gptClient.appendInputAudio(int16Array);
            
            // Debug audio flow (log 1% of packets to avoid spam)
            if (Math.random() < 0.01) {
                console.log(`Audio forwarded to GPT: ${int16Array.length} samples`);
            }
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
        const ratio = fromRate / toRate;
        const inputSamples = new Int16Array(pcm16Buffer);
        const outputLength = Math.floor(inputSamples.length / ratio);
        const outputSamples = new Int16Array(outputLength);
        
        for (let i = 0; i < outputLength; i++) {
            const srcIndex = Math.floor(i * ratio);
            outputSamples[i] = inputSamples[srcIndex] || 0;
        }
        
        return outputSamples.buffer;
    }

    pcm16ToPcmu(pcm16Buffer) {
        const BIAS = 0x84;
        const CLIP = 8159;
        const pcmSamples = new Int16Array(pcm16Buffer);
        const pcmuBuffer = Buffer.alloc(pcmSamples.length);
        
        for (let i = 0; i < pcmSamples.length; i++) {
            let sample = Math.max(-CLIP, Math.min(CLIP, pcmSamples[i]));
            const sign = (sample < 0) ? 0x80 : 0x00;
            if (sample < 0) sample = -sample;
            sample += BIAS;
            
            let exponent = 0;
            if (sample >= 256) {
                exponent = 1;
                sample >>= 1;
            }
            if (sample >= 256) {
                exponent = 2;
                sample >>= 1;
            }
            if (sample >= 256) {
                exponent = 3;
                sample >>= 1;
            }
            if (sample >= 256) {
                exponent = 4;
                sample >>= 1;
            }
            if (sample >= 256) {
                exponent = 5;
                sample >>= 1;
            }
            if (sample >= 256) {
                exponent = 6;
                sample >>= 1;
            }
            if (sample >= 256) {
                exponent = 7;
                sample >>= 1;
            }
            
            const mantissa = (sample >> 4) & 0x0F;
            pcmuBuffer[i] = sign | (exponent << 4) | mantissa;
        }
        
        return pcmuBuffer;
    }

    createRtpPacket(payloadBuffer) {
        // Create minimal RTP header (12 bytes)
        const rtpHeader = Buffer.alloc(12);
        rtpHeader[0] = 0x80; // Version 2, no padding, no extension, no CSRC
        rtpHeader[1] = 0x00; // PCMU payload type
        // Sequence number and timestamp would be managed properly in production
        rtpHeader.writeUInt16BE(Math.floor(Math.random() * 65536), 2); // Random sequence
        rtpHeader.writeUInt32BE(Date.now(), 4); // Simple timestamp
        rtpHeader.writeUInt32BE(0x12345678, 8); // SSRC identifier
        
        return Buffer.concat([rtpHeader, payloadBuffer]);
    }

    streamGPTAudioToTelnyx(callData, audioDelta) {
        try {
            // audioDelta is base64-encoded PCM16 audio from GPT
            if (!audioDelta || !callData.ws) return;
            
            // Decode GPT's PCM16 audio
            const pcm16Buffer = Buffer.from(audioDelta, 'base64');
            
            // Downsample from 24kHz to 8kHz for Telnyx
            const downsampledBuffer = this.downsampleAudio(pcm16Buffer, 24000, 8000);
            
            // Convert PCM16 to PCMU (μ-law)
            const pcmuBuffer = this.pcm16ToPcmu(downsampledBuffer);
            
            // Create RTP header and payload
            const rtpPacket = this.createRtpPacket(pcmuBuffer);
            
            // Send to Telnyx WebSocket
            callData.ws.send(JSON.stringify({
                event: 'media',
                stream_id: callData.streamId,
                media: {
                    payload: rtpPacket.toString('base64')
                }
            }));
            
            console.log(`Sent GPT audio to Telnyx: ${pcmuBuffer.length} bytes`);
            
        } catch (error) {
            console.error('Error streaming GPT audio to Telnyx:', error);
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
        if (callData.gptClient) {
            try {
                callData.gptClient.disconnect();
            } catch (error) {
                console.error('Error disconnecting GPT client:', error);
            }
        }
        if (callData.streamId) {
            this.activeCalls.delete(callData.streamId);
        }
    }
}

export default TelnyxGPTGateway;