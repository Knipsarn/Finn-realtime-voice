/**
 * Telnyx Media Streams to GPT-Realtime WebRTC Gateway
 * Handles bidirectional audio streaming and transcoding from RTP/PCMU to PCM16
 */

import { WebSocketServer } from 'ws';
import OpenAI from 'openai';
import { OpenAIRealtimeWebSocket } from 'openai/beta/realtime/websocket';
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
        switch (message.event) {
            case 'connected':
                console.log('Telnyx stream connected, version:', message.version);
                break;

            case 'start':
                callData.streamId = message.stream_id;
                console.log(`Stream started with ID: ${callData.streamId}`);
                console.log('Media format:', message.start.media_format);
                
                // Get default agent for now - in production you'd pass this via URL params
                const agents = await database.getAllAgents();
                callData.agent = agents[0];
                
                if (callData.agent) {
                    console.log(`Using agent ${callData.agent.id}: "${callData.agent.voice}"`);
                    // Initialize GPT-Realtime session
                    await this.initializeGPTSession(callData);
                    this.activeCalls.set(callData.streamId, callData);
                }
                break;

            case 'media':
                // Transcode and forward audio to GPT-Realtime
                if (callData.gptClient) {
                    const audioData = this.transcodeToGPT(message.media.payload);
                    if (audioData) {
                        await callData.gptClient.appendInputAudio(audioData);
                    }
                }
                break;

            case 'stop':
                console.log(`Stream ${callData.streamId} ended by Telnyx`);
                await this.endCall(callData);
                break;
        }
    }

    async initializeGPTSession(callData) {
        try {
            // Initialize OpenAI Realtime WebSocket connection
            callData.gptClient = new OpenAIRealtimeWebSocket({
                apiKey: process.env.OPENAI_API_KEY,
                model: 'gpt-realtime'
            });

            // Configure session
            await callData.gptClient.updateSession({
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

            // Set up event handlers
            callData.gptClient.on('response.audio.delta', (event) => {
                // Forward audio response back to Telnyx
                this.streamGPTAudioToTelnyx(callData, event.delta);
            });

            callData.gptClient.on('response.text.delta', (event) => {
                console.log('GPT speaking:', event.delta);
            });

            callData.gptClient.on('input_audio_buffer.speech_started', () => {
                console.log('User started speaking');
            });

            callData.gptClient.on('input_audio_buffer.speech_stopped', () => {
                console.log('User stopped speaking');
            });

            callData.gptClient.on('error', (error) => {
                console.error('GPT error:', error);
            });

            // Connect to OpenAI
            await callData.gptClient.connect();

            // Send initial greeting
            await callData.gptClient.sendUserMessageContent([{
                type: 'input_text',
                text: this.getGreeting(callData.agent)
            }]);

            console.log('GPT-Realtime connection established for stream', callData.streamId);

        } catch (error) {
            console.error('Failed to initialize GPT session:', error);
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
            callData.gptClient.disconnect();
        }
        if (callData.streamId) {
            this.activeCalls.delete(callData.streamId);
        }
    }
}

export default TelnyxGPTGateway;