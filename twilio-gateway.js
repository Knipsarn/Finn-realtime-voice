/**
 * Twilio Media Streams to GPT-Realtime WebRTC Gateway
 * Handles bidirectional audio streaming and transcoding
 */

import { WebSocketServer } from 'ws';
import alawmulaw from 'alawmulaw';
import OpenAI from 'openai';
import database from './database.js';

const { decode: mulawDecode } = alawmulaw;

class TwilioGPTGateway {
    constructor(server) {
        this.server = server;
        this.openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
        this.activeCalls = new Map(); // callId -> { ws, gptConnection, agent }
        
        this.initializeWebSocketServer();
    }

    initializeWebSocketServer() {
        this.wss = new WebSocketServer({ 
            server: this.server,
            path: '/api/twilio/stream'
        });

        this.wss.on('connection', (ws, req) => {
            console.log('Twilio Media Stream connected');
            this.handleTwilioConnection(ws, req);
        });

        console.log('Twilio WebSocket server initialized on /api/twilio/stream');
    }

    async handleTwilioConnection(ws, req) {
        let callData = {
            callId: null,
            agentId: null,
            agent: null,
            streamSid: null,
            gptSession: null,
            peerConnection: null,
            dataChannel: null,
            audioContext: null,
            startTime: Date.now()
        };

        ws.on('message', async (data) => {
            try {
                const message = JSON.parse(data.toString());
                await this.handleTwilioMessage(ws, message, callData);
            } catch (error) {
                console.error('Error handling Twilio message:', error);
            }
        });

        ws.on('close', () => {
            console.log(`Twilio stream closed for call ${callData.callId}`);
            this.cleanup(callData);
        });

        ws.on('error', (error) => {
            console.error('Twilio WebSocket error:', error);
            this.cleanup(callData);
        });
    }

    async handleTwilioMessage(ws, message, callData) {
        switch (message.event) {
            case 'connected':
                console.log('Twilio stream connected');
                break;

            case 'start':
                callData.streamSid = message.streamSid;
                callData.callId = message.start.customParameters?.callId;
                callData.agentId = message.start.customParameters?.agentId;
                
                if (callData.agentId) {
                    callData.agent = await database.getAgent(callData.agentId);
                    console.log(`Call ${callData.callId} started with agent ${callData.agentId}: "${callData.agent.voice}"`);
                    
                    // Initialize GPT-Realtime session
                    await this.initializeGPTSession(callData);
                    this.activeCalls.set(callData.callId, callData);
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
                console.log(`Call ${callData.callId} ended by Twilio`);
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
                // Convert PCM16 audio to μlaw and send to Twilio
                this.streamGPTAudioToTwilio(callData, event.streams[0]);
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

            console.log('GPT-Realtime connection established for call', callData.callId);

        } catch (error) {
            console.error('Failed to initialize GPT session:', error);
        }
    }

    transcodeToGPT(base64MulawAudio) {
        try {
            // Decode base64 to buffer
            const mulawBuffer = Buffer.from(base64MulawAudio, 'base64');
            
            // Decode μlaw to 16-bit PCM
            const pcmBuffer = mulawDecode(mulawBuffer);
            
            // Convert to PCM16 at 24kHz (basic upsampling)
            // This is a simplified conversion - in production you'd want proper resampling
            const upsampledBuffer = this.upsampleAudio(pcmBuffer, 8000, 24000);
            
            return Buffer.from(upsampledBuffer).toString('base64');
        } catch (error) {
            console.error('Audio transcoding error:', error);
            return null;
        }
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

    streamGPTAudioToTwilio(callData, audioStream) {
        // This would handle streaming GPT audio back to Twilio
        // For now, we'll implement basic audio forwarding
        console.log('GPT audio received - would transcode back to μlaw for Twilio');
        
        // TODO: Implement PCM16 → μlaw transcoding and WebSocket send to Twilio
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
        if (callData.callId) {
            const duration = Date.now() - callData.startTime;
            await database.endCall(callData.callId, null, duration);
            console.log(`Call ${callData.callId} ended, duration: ${duration}ms`);
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
        if (callData.callId) {
            this.activeCalls.delete(callData.callId);
        }
    }
}

export default TwilioGPTGateway;