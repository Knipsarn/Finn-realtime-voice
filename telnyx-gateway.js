/**
 * Telnyx Media Streams to GPT-Realtime WebRTC Gateway
 * Handles bidirectional audio streaming and transcoding from RTP/PCMU to PCM16
 */

import { WebSocketServer } from 'ws';
import OpenAI from 'openai';
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
            console.log('Telnyx Media Stream connected');
            this.handleTelnyxConnection(ws, req);
        });

        console.log('Telnyx WebSocket server initialized on /api/telnyx/stream');
    }

    async handleTelnyxConnection(ws, req) {
        let callData = {
            callControlId: null,
            streamId: null,
            agentId: null,
            agent: null,
            gptSession: null,
            peerConnection: null,
            dataChannel: null,
            audioContext: null,
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
                if (callData.dataChannel && callData.dataChannel.readyState === 'open') {
                    const audioData = this.transcodeToGPT(message.media.payload);
                    if (audioData) {
                        callData.dataChannel.send(JSON.stringify({
                            type: 'input_audio_buffer.append',
                            audio: audioData
                        }));
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
            // Create ephemeral session
            const sessionResponse = await this.openai.beta.realtime.sessions.create({
                model: 'gpt-realtime',
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

            // Initialize WebRTC connection
            callData.peerConnection = new RTCPeerConnection({
                iceServers: [{ urls: 'stun:stun.l.google.com:19302' }]
            });

            callData.dataChannel = callData.peerConnection.createDataChannel('oai-events', {
                ordered: true
            });

            callData.dataChannel.onopen = () => {
                console.log('GPT-Realtime data channel opened');
                // Send initial conversation trigger
                callData.dataChannel.send(JSON.stringify({
                    type: 'conversation.item.create',
                    item: {
                        type: 'message',
                        role: 'assistant',
                        content: [{
                            type: 'text',
                            text: this.getGreeting(callData.agent)
                        }]
                    }
                }));

                callData.dataChannel.send(JSON.stringify({
                    type: 'response.create'
                }));
            };

            callData.dataChannel.onmessage = (event) => {
                try {
                    const gptMessage = JSON.parse(event.data);
                    this.handleGPTMessage(callData, gptMessage);
                } catch (error) {
                    console.error('Error parsing GPT message:', error);
                }
            };

            // Handle audio from GPT
            callData.peerConnection.ontrack = (event) => {
                console.log('Received GPT audio track');
                // Convert PCM16 audio to RTP/PCMU and send to Telnyx
                this.streamGPTAudioToTelnyx(callData, event.streams[0]);
            };

            // Create WebRTC offer
            const offer = await callData.peerConnection.createOffer();
            await callData.peerConnection.setLocalDescription(offer);

            // Send to OpenAI
            const response = await fetch('https://api.openai.com/v1/realtime', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${sessionResponse.client_secret.value}`,
                    'Content-Type': 'application/sdp'
                },
                body: offer.sdp
            });

            if (!response.ok) {
                throw new Error(`WebRTC negotiation failed: ${response.status}`);
            }

            const answerSdp = await response.text();
            await callData.peerConnection.setRemoteDescription({
                type: 'answer',
                sdp: answerSdp
            });

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

    streamGPTAudioToTelnyx(callData, audioStream) {
        // TODO: Implement PCM16 → RTP/PCMU transcoding and WebSocket send to Telnyx
        console.log('GPT audio received - would transcode back to RTP/PCMU for Telnyx');
    }

    handleGPTMessage(callData, message) {
        console.log('GPT message:', message.type);
        
        switch (message.type) {
            case 'response.audio_transcript.delta':
                console.log('GPT speaking:', message.delta);
                break;
            case 'input_audio_buffer.speech_started':
                console.log('User started speaking');
                break;
            case 'input_audio_buffer.speech_stopped':
                console.log('User stopped speaking');
                break;
            case 'error':
                console.error('GPT error:', message.error);
                break;
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
        if (callData.dataChannel) {
            callData.dataChannel.close();
        }
        if (callData.peerConnection) {
            callData.peerConnection.close();
        }
        if (callData.streamId) {
            this.activeCalls.delete(callData.streamId);
        }
    }
}

export default TelnyxGPTGateway;