/**
 * GPT-Realtime Voice Test Harness Client
 * WebRTC + OpenAI Realtime API integration for Swedish voice testing
 */

class RealtimeVoiceClient {
    constructor() {
        this.peerConnection = null;
        this.dataChannel = null;
        this.audioContext = null;
        this.mediaStream = null;
        this.sessionData = null;
        this.isConnected = false;
        this.isMuted = false;
        this.latencyTracker = {
            startTime: null,
            samples: [],
            averageLatency: 0
        };
        
        this.initializeUI();
        this.bindEvents();
    }

    initializeUI() {
        this.elements = {
            connectBtn: document.getElementById('connect-btn'),
            disconnectBtn: document.getElementById('disconnect-btn'),
            muteBtn: document.getElementById('mute-btn'),
            unmuteBtn: document.getElementById('unmute-btn'),
            systemPrompt: document.getElementById('system-prompt'),
            voiceSelect: document.getElementById('voice-select'),
            localeSelect: document.getElementById('locale-select'),
            connectionValue: document.getElementById('connection-value'),
            audioValue: document.getElementById('audio-value'),
            latencyValue: document.getElementById('latency-value'),
            transcript: document.getElementById('transcript'),
            audioOutput: document.getElementById('audio-output'),
            sessionInfo: document.getElementById('session-info'),
            performanceInfo: document.getElementById('performance-info')
        };
    }

    bindEvents() {
        this.elements.connectBtn.addEventListener('click', () => this.connect());
        this.elements.disconnectBtn.addEventListener('click', () => this.disconnect());
        this.elements.muteBtn.addEventListener('click', () => this.mute());
        this.elements.unmuteBtn.addEventListener('click', () => this.unmute());
    }

    async connect() {
        try {
            this.updateStatus('Connecting...', 'connecting');
            this.addTranscript('system', 'Initializing session...');

            // Request microphone permission
            this.mediaStream = await navigator.mediaDevices.getUserMedia({ 
                audio: {
                    sampleRate: 24000,
                    channelCount: 1,
                    echoCancellation: true,
                    noiseSuppression: true
                } 
            });

            // Create session with server (ephemeral token)
            const sessionResponse = await fetch('/api/session', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    voice: this.elements.voiceSelect.value,
                    locale: this.elements.localeSelect.value,
                    instructions: this.elements.systemPrompt.value.trim() || this.getInstructions(),
                    firstLine: this.getFirstLine()
                })
            });

            if (!sessionResponse.ok) {
                const error = await sessionResponse.json();
                throw new Error(error.error || 'Failed to create session');
            }

            this.sessionData = await sessionResponse.json();
            this.addTranscript('system', `Session created: ${this.sessionData.session.session_id}`);

            // Initialize WebRTC connection with ephemeral token
            await this.initializeWebRTC();
            
            // Initialize audio context
            await this.initializeAudioContext();

            this.isConnected = true;
            this.updateUI();
            this.updateStatus('Connected', 'connected');
            this.addTranscript('system', 'Ready for voice conversation');

        } catch (error) {
            console.error('Connection failed:', error);
            this.addTranscript('error', `Connection failed: ${error.message}`);
            this.updateStatus('Connection failed', 'error');
            this.cleanup();
        }
    }

    async initializeWebRTC() {
        return new Promise(async (resolve, reject) => {
            try {
                // Extract ephemeral token from session response
                const ephemeralToken = this.sessionData.session.client_secret.value;
                
                this.addTranscript('system', 'Initializing WebRTC connection...');
                
                // Create RTCPeerConnection
                this.peerConnection = new RTCPeerConnection({
                    iceServers: [{ urls: 'stun:stun.l.google.com:19302' }]
                });

                // Create data channel for sending/receiving messages
                this.dataChannel = this.peerConnection.createDataChannel('oai-events', {
                    ordered: true
                });

                // Handle data channel events
                this.dataChannel.onopen = () => {
                    this.addTranscript('system', 'WebRTC data channel connected');
                    this.startConversation();
                };

                this.dataChannel.onmessage = (event) => {
                    try {
                        const message = JSON.parse(event.data);
                        this.handleRealtimeMessage(message);
                    } catch (error) {
                        console.error('Failed to parse data channel message:', error);
                    }
                };

                // Add audio tracks to peer connection
                this.mediaStream.getAudioTracks().forEach(track => {
                    this.peerConnection.addTrack(track, this.mediaStream);
                });

                // Handle incoming audio
                this.peerConnection.ontrack = (event) => {
                    console.log('Received remote audio track');
                    const remoteAudio = document.getElementById('audio-output');
                    if (remoteAudio) {
                        remoteAudio.srcObject = event.streams[0];
                    }
                };

                // Create offer
                const offer = await this.peerConnection.createOffer();
                await this.peerConnection.setLocalDescription(offer);

                // Send offer to OpenAI Realtime API
                const response = await fetch('https://api.openai.com/v1/realtime', {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${ephemeralToken}`,
                        'Content-Type': 'application/sdp'
                    },
                    body: offer.sdp
                });

                if (!response.ok) {
                    throw new Error(`WebRTC negotiation failed: ${response.status} ${response.statusText}`);
                }

                const answerSdp = await response.text();
                await this.peerConnection.setRemoteDescription({
                    type: 'answer',
                    sdp: answerSdp
                });

                this.addTranscript('system', 'WebRTC connection established');
                resolve();

            } catch (error) {
                console.error('WebRTC initialization failed:', error);
                reject(error);
            }
        });
    }

    async initializeAudioContext() {
        this.audioContext = new (window.AudioContext || window.webkitAudioContext)({
            sampleRate: 24000
        });

        // Set up audio input processing
        const source = this.audioContext.createMediaStreamSource(this.mediaStream);
        const processor = this.audioContext.createScriptProcessor(4096, 1, 1);
        
        processor.onaudioprocess = (event) => {
            if (!this.isMuted && this.ws && this.ws.readyState === WebSocket.OPEN) {
                const inputBuffer = event.inputBuffer.getChannelData(0);
                const pcm16 = this.convertToPCM16(inputBuffer);
                
                this.ws.send(JSON.stringify({
                    type: 'input_audio_buffer.append',
                    audio: this.arrayBufferToBase64(pcm16)
                }));
            }
        };

        source.connect(processor);
        processor.connect(this.audioContext.destination);

        this.updateStatus('', 'active', 'audio');
    }

    convertToPCM16(float32Array) {
        const pcm16 = new Int16Array(float32Array.length);
        for (let i = 0; i < float32Array.length; i++) {
            const s = Math.max(-1, Math.min(1, float32Array[i]));
            pcm16[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
        }
        return pcm16.buffer;
    }

    arrayBufferToBase64(buffer) {
        const bytes = new Uint8Array(buffer);
        let binary = '';
        for (let i = 0; i < bytes.byteLength; i++) {
            binary += String.fromCharCode(bytes[i]);
        }
        return btoa(binary);
    }

    handleRealtimeMessage(message) {
        console.log('Received:', message.type);

        switch (message.type) {
            case 'response.audio_transcript.delta':
                this.addTranscript('assistant', message.delta, true);
                break;

            case 'response.audio.delta':
                // Audio is handled via WebRTC audio track, not manual playback
                break;

            case 'input_audio_buffer.speech_started':
                this.latencyTracker.startTime = Date.now();
                this.addTranscript('user', '[Speaking...]', true);
                break;

            case 'input_audio_buffer.speech_stopped':
                this.addTranscript('user', '[Stopped speaking]', false);
                break;

            case 'response.done':
                this.trackLatency();
                break;

            case 'error':
                this.addTranscript('error', `Error: ${message.error.message}`);
                break;

            default:
                console.log('Unhandled message type:', message.type);
        }
    }

    playAudioDelta(base64Audio) {
        try {
            const binaryString = atob(base64Audio);
            const bytes = new Uint8Array(binaryString.length);
            for (let i = 0; i < binaryString.length; i++) {
                bytes[i] = binaryString.charCodeAt(i);
            }
            
            const audioBuffer = this.audioContext.createBuffer(1, bytes.length / 2, 24000);
            const channelData = audioBuffer.getChannelData(0);
            
            const dataView = new DataView(bytes.buffer);
            for (let i = 0; i < channelData.length; i++) {
                channelData[i] = dataView.getInt16(i * 2, true) / 0x8000;
            }
            
            const source = this.audioContext.createBufferSource();
            source.buffer = audioBuffer;
            source.connect(this.audioContext.destination);
            source.start();
            
        } catch (error) {
            console.error('Audio playback error:', error);
        }
    }

    startConversation() {
        if (this.dataChannel && this.dataChannel.readyState === 'open') {
            // Session is already configured server-side via ephemeral token
            // Just send the initial conversation item and trigger response
            this.dataChannel.send(JSON.stringify({
                type: 'conversation.item.create',
                item: {
                    type: 'message',
                    role: 'assistant', 
                    content: [
                        {
                            type: 'text',
                            text: this.getFirstLine()
                        }
                    ]
                }
            }));

            this.dataChannel.send(JSON.stringify({
                type: 'response.create'
            }));
        }
    }

    getInstructions() {
        const locale = this.elements.localeSelect.value;
        if (locale === 'sv-SE') {
            return 'Du är en hjälpsam AI-assistent som talar svenska. Svara naturligt och använd svenska konversationsmönster. Var kortfattad om inte användaren ber om detaljerad information.';
        }
        return 'You are a helpful AI assistant. Respond naturally and be concise unless the user asks for detailed information.';
    }

    getFirstLine() {
        const locale = this.elements.localeSelect.value;
        if (locale === 'sv-SE') {
            return 'Hej! Vad kan jag hjälpa dig med idag?';
        }
        return 'Hello! How can I help you today?';
    }

    trackLatency() {
        if (this.latencyTracker.startTime) {
            const latency = Date.now() - this.latencyTracker.startTime;
            this.latencyTracker.samples.push(latency);
            
            if (this.latencyTracker.samples.length > 10) {
                this.latencyTracker.samples.shift();
            }
            
            this.latencyTracker.averageLatency = 
                this.latencyTracker.samples.reduce((a, b) => a + b, 0) / 
                this.latencyTracker.samples.length;
            
            this.elements.latencyValue.textContent = `${Math.round(this.latencyTracker.averageLatency)}ms`;
            this.latencyTracker.startTime = null;
        }
    }

    mute() {
        this.isMuted = true;
        this.updateUI();
        this.addTranscript('system', 'Microphone muted');
    }

    unmute() {
        this.isMuted = false;
        this.updateUI();
        this.addTranscript('system', 'Microphone unmuted');
    }

    disconnect() {
        this.addTranscript('system', 'Disconnecting...');
        this.cleanup();
        this.updateStatus('Disconnected', 'disconnected');
        this.addTranscript('system', 'Disconnected');
    }

    cleanup() {
        if (this.dataChannel) {
            this.dataChannel.close();
            this.dataChannel = null;
        }
        
        if (this.peerConnection) {
            this.peerConnection.close();
            this.peerConnection = null;
        }
        
        if (this.mediaStream) {
            this.mediaStream.getTracks().forEach(track => track.stop());
            this.mediaStream = null;
        }
        
        if (this.audioContext) {
            this.audioContext.close();
            this.audioContext = null;
        }
        
        this.isConnected = false;
        this.isMuted = false;
        this.sessionData = null;
        this.updateUI();
    }

    updateUI() {
        this.elements.connectBtn.disabled = this.isConnected;
        this.elements.disconnectBtn.disabled = !this.isConnected;
        this.elements.muteBtn.disabled = !this.isConnected || this.isMuted;
        this.elements.unmuteBtn.disabled = !this.isConnected || !this.isMuted;
        
        this.elements.systemPrompt.disabled = this.isConnected;
        this.elements.voiceSelect.disabled = this.isConnected;
        this.elements.localeSelect.disabled = this.isConnected;
    }

    updateStatus(connectionStatus, connectionClass, type = 'connection') {
        if (type === 'connection') {
            this.elements.connectionValue.textContent = connectionStatus;
            this.elements.connectionValue.className = `value ${connectionClass}`;
        } else if (type === 'audio') {
            this.elements.audioValue.textContent = this.isMuted ? 'Muted' : 'Active';
            this.elements.audioValue.className = `value ${this.isMuted ? 'muted' : 'active'}`;
        }
    }

    addTranscript(role, message, isPartial = false) {
        const transcript = this.elements.transcript;
        
        if (isPartial) {
            let existingMessage = transcript.querySelector(`[data-role="${role}"][data-partial="true"]`);
            if (existingMessage) {
                existingMessage.textContent += message;
                return;
            }
        }
        
        const messageElement = document.createElement('div');
        messageElement.className = `message ${role}`;
        messageElement.setAttribute('data-role', role);
        messageElement.setAttribute('data-partial', isPartial.toString());
        messageElement.textContent = `${role === 'user' ? 'You' : role === 'assistant' ? 'Assistant' : 'System'}: ${message}`;
        
        transcript.appendChild(messageElement);
        transcript.scrollTop = transcript.scrollHeight;
    }
}

// Initialize the client when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.realtimeClient = new RealtimeVoiceClient();
});