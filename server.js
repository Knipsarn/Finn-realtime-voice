#!/usr/bin/env node
/**
 * GPT-Realtime Voice Test Harness Server
 * Provides endpoints for session management and static file serving
 */

import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { createServer } from 'http';
import OpenAI from 'openai';
import 'dotenv/config';
import database from './database.js';
import TwilioGPTGateway from './twilio-gateway.js';
import TelnyxGPTGateway from './telnyx-gateway.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3010;

// Initialize OpenAI client
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY
});

// Initialize database
await database.initialize();

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// T014: GET / - Serve static client
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// T015: GET /api/config - Return Swedish configuration defaults
app.get('/api/config', (req, res) => {
    const swedishDefaults = {
        audio: {
            voices: ['alloy', 'echo', 'fable', 'onyx', 'nova', 'shimmer', 'cedar', 'marin'],
            defaultVoice: 'alloy',
            locales: ['sv-SE', 'en-US'],
            defaultLocale: 'sv-SE',
            sampleRate: 24000,
            format: 'pcm16'
        },
        conversation: {
            styles: ['concise', 'verbose'],
            defaultStyle: 'concise',
            defaultInstructions: 'Du är en hjälpsam AI-assistent som talar svenska. Svara naturligt och använd svenska konversationsmönster. Var kortfattad om inte användaren ber om detaljerad information.',
            defaultFirstLine: 'Hej! Vad kan jag hjälpa dig med idag?',
            maxTurnDuration: 30000,
            silenceTimeout: 3000
        },
        performance: {
            targetLatency: 800,
            jitterThreshold: 100,
            qualityThresholds: {
                excellent: 560,
                good: 800,
                acceptable: 1200
            }
        },
        features: {
            bargeInEnabled: true,
            languageSwitching: true,
            realTimeConfig: true,
            performanceTracking: true
        },
        culturalContext: {
            country: 'Sweden',
            timezone: 'Europe/Stockholm',
            currency: 'SEK',
            dateFormat: 'YYYY-MM-DD',
            timeFormat: '24h'
        }
    };
    
    res.json({
        success: true,
        config: swedishDefaults,
        version: '1.0.0',
        timestamp: new Date().toISOString()
    });
});

// Agent Management API Endpoints

// GET /api/agents - List all agents
app.get('/api/agents', async (req, res) => {
    try {
        const agents = await database.getAllAgents();
        res.json({
            success: true,
            agents,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error fetching agents:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch agents',
            details: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
});

// POST /api/agents - Create new agent
app.post('/api/agents', async (req, res) => {
    try {
        const { prompt, voice } = req.body;
        
        if (!prompt || !voice) {
            return res.status(400).json({
                success: false,
                error: 'Missing required parameters: prompt and voice are required'
            });
        }

        const agentId = await database.createAgent(prompt, voice);
        const agent = await database.getAgent(agentId);
        
        res.json({
            success: true,
            agent,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error creating agent:', error);
        res.status(400).json({
            success: false,
            error: error.message || 'Failed to create agent'
        });
    }
});

// GET /api/agents/:id - Get specific agent
app.get('/api/agents/:id', async (req, res) => {
    try {
        const agent = await database.getAgent(req.params.id);
        if (!agent) {
            return res.status(404).json({
                success: false,
                error: 'Agent not found'
            });
        }
        
        res.json({
            success: true,
            agent,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error fetching agent:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch agent'
        });
    }
});

// PUT /api/agents/:id - Update agent
app.put('/api/agents/:id', async (req, res) => {
    try {
        const { prompt, voice } = req.body;
        
        if (!prompt || !voice) {
            return res.status(400).json({
                success: false,
                error: 'Missing required parameters: prompt and voice are required'
            });
        }

        const success = await database.updateAgent(req.params.id, prompt, voice);
        if (!success) {
            return res.status(404).json({
                success: false,
                error: 'Agent not found'
            });
        }

        const agent = await database.getAgent(req.params.id);
        res.json({
            success: true,
            agent,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error updating agent:', error);
        res.status(400).json({
            success: false,
            error: error.message || 'Failed to update agent'
        });
    }
});

// GET /api/calls - List recent calls
app.get('/api/calls', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 50;
        const calls = await database.getRecentCalls(limit);
        res.json({
            success: true,
            calls,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error fetching calls:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch calls'
        });
    }
});

// GET /api/calls/:id - Get specific call
app.get('/api/calls/:id', async (req, res) => {
    try {
        const call = await database.getCall(req.params.id);
        if (!call) {
            return res.status(404).json({
                success: false,
                error: 'Call not found'
            });
        }
        
        res.json({
            success: true,
            call,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error fetching call:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch call'
        });
    }
});

// T016: POST /api/session - Create OpenAI Realtime session with ephemeral token
app.post('/api/session', async (req, res) => {
    try {
        // Validate request body
        const { voice, locale, instructions, firstLine, style } = req.body;
        
        if (!voice || !locale) {
            return res.status(400).json({
                success: false,
                error: 'Missing required parameters: voice and locale are required'
            });
        }
        
        // Validate voice parameter
        const validVoices = ['alloy', 'echo', 'fable', 'onyx', 'nova', 'shimmer', 'cedar', 'marin'];
        if (!validVoices.includes(voice)) {
            return res.status(400).json({
                success: false,
                error: `Invalid voice. Must be one of: ${validVoices.join(', ')}`
            });
        }
        
        // Validate locale parameter
        const validLocales = ['sv-SE', 'en-US'];
        if (!validLocales.includes(locale)) {
            return res.status(400).json({
                success: false,
                error: `Invalid locale. Must be one of: ${validLocales.join(', ')}`
            });
        }
        
        // Create ephemeral token for realtime session
        const response = await openai.beta.realtime.sessions.create({
            model: 'gpt-realtime',
            voice: voice,
            instructions: instructions || 'Du är en hjälpsam AI-assistent som talar svenska. Svara naturligt och använd svenska konversationsmönster.',
            input_audio_format: 'pcm16',
            output_audio_format: 'pcm16',
            input_audio_transcription: {
                model: 'whisper-1'
            },
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
        
        // Generate session ID
        const sessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
        // Return session data for client
        const sessionData = {
            session_id: sessionId,
            client_secret: response.client_secret,
            websocket_url: `wss://api.openai.com/v1/realtime?model=gpt-realtime`,
            configuration: {
                voice,
                locale,
                instructions: instructions || 'Du är en hjälpsam AI-assistent som talar svenska.',
                firstLine: firstLine || 'Hej! Vad kan jag hjälpa dig med idag?',
                style: style || 'concise'
            },
            expires_at: Date.now() + (60 * 60 * 1000), // 1 hour
            created_at: new Date().toISOString()
        };
        
        console.log(`Created session ${sessionId} for voice=${voice}, locale=${locale}`);
        
        res.json({
            success: true,
            session: sessionData,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('Session creation error:', error);
        
        // Handle OpenAI API errors
        if (error.status === 401) {
            return res.status(500).json({
                success: false,
                error: 'OpenAI API authentication failed. Check OPENAI_API_KEY.',
                code: 'AUTH_ERROR'
            });
        }
        
        if (error.status === 429) {
            return res.status(429).json({
                success: false,
                error: 'OpenAI API rate limit exceeded. Please try again later.',
                code: 'RATE_LIMIT'
            });
        }
        
        return res.status(500).json({
            success: false,
            error: 'Failed to create session',
            code: 'SESSION_ERROR',
            details: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
});

// Twilio Voice Webhook - Incoming Call Handler
app.post('/api/twilio/voice', async (req, res) => {
    try {
        console.log('Incoming call from:', req.body.From, 'to:', req.body.To);
        
        // Create call record in database
        const agents = await database.getAllAgents();
        const defaultAgent = agents[0]; // Use first agent as default
        
        if (!defaultAgent) {
            console.error('No agents configured');
            return res.status(500).send('<Response><Say>No agents configured</Say></Response>');
        }

        const callId = await database.createCall(defaultAgent.id, req.body.From, req.body.To);
        console.log(`Created call record ${callId} using agent ${defaultAgent.id}`);

        // Return TwiML to connect to our WebSocket stream
        const streamUrl = `wss://${req.get('host')}/api/twilio/stream`;
        
        const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Connect>
        <Stream url="${streamUrl}">
            <Parameter name="callId" value="${callId}" />
            <Parameter name="agentId" value="${defaultAgent.id}" />
        </Stream>
    </Connect>
</Response>`;

        res.type('text/xml');
        res.send(twimlResponse);
        
    } catch (error) {
        console.error('Twilio webhook error:', error);
        res.status(500).send('<Response><Say>Internal server error</Say></Response>');
    }
});

// Telnyx Voice Webhook - Incoming Call Handler
app.post('/api/telnyx/voice', async (req, res) => {
    try {
        console.log('=== TELNYX WEBHOOK DEBUG ===');
        console.log('Raw body:', JSON.stringify(req.body, null, 2));
        console.log('Headers:', JSON.stringify(req.headers, null, 2));
        
        const event = req.body.data;
        if (!event) {
            console.log('ERROR: No data field in webhook');
            return res.status(400).json({ error: 'No data field' });
        }
        
        console.log('Event type:', event.event_type);
        
        if (event.event_type === 'call.initiated') {
            const payload = event.payload;
            console.log('Incoming call from:', payload.from, 'to:', payload.to);
            
            // Create call record in database
            const agents = await database.getAllAgents();
            const defaultAgent = agents[0]; // Use first agent as default
            
            if (!defaultAgent) {
                console.error('No agents configured');
                return res.status(500).json({ 
                    data: { 
                        command: 'hangup',
                        call_control_id: payload.call_control_id 
                    }
                });
            }

            const callId = await database.createCall(defaultAgent.id, payload.from, payload.to);
            console.log(`Created call record ${callId} using agent ${defaultAgent.id}`);

            // Answer the call and start media streaming
            const streamUrl = `wss://${req.get('host')}/api/telnyx/stream`;
            
            const response = {
                data: [
                    {
                        command: 'answer',
                        call_control_id: payload.call_control_id
                    },
                    {
                        command: 'streaming_start',
                        call_control_id: payload.call_control_id,
                        stream_url: streamUrl,
                        stream_track: 'both_tracks',
                        stream_bidirectional_mode: 'rtp'
                    }
                ]
            };
            
            res.json(response);
        } else {
            // Acknowledge other events
            res.status(200).json({});
        }
        
    } catch (error) {
        console.error('Telnyx webhook error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// WebSocket test endpoint
app.get('/api/telnyx/stream', (req, res) => {
    res.json({ 
        error: 'This is a WebSocket endpoint', 
        upgrade_required: true,
        websocket_url: 'wss://web-production-b99cf.up.railway.app/api/telnyx/stream'
    });
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ 
        status: 'ok', 
        service: 'gpt-realtime-voice-test-harness-twilio',
        openai_configured: !!process.env.OPENAI_API_KEY,
        database_connected: !!database.db,
        twilio_configured: !!(process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN),
        timestamp: new Date().toISOString()
    });
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('Shutting down server...');
    database.close();
    process.exit(0);
});

// Create HTTP server and initialize gateways
const httpServer = createServer(app);
const twilioGateway = new TwilioGPTGateway(httpServer);
const telnyxGateway = new TelnyxGPTGateway(httpServer);

// Start server
httpServer.listen(PORT, () => {
    console.log(`Twilio GPT-Realtime Gateway running on http://localhost:${PORT}`);
    console.log('Static client available at GET /');
    console.log('API endpoints:');
    console.log('  GET /health - Service health check');
    console.log('  GET /api/config - Configuration presets');
    console.log('  POST /api/session - GPT-Realtime session creation');
    console.log('  POST /api/twilio/voice - Twilio voice webhook');
    console.log('  WS /api/twilio/stream - Twilio media stream handler');
    console.log('  POST /api/telnyx/voice - Telnyx voice webhook');
    console.log('  WS /api/telnyx/stream - Telnyx media stream handler');
    console.log('  GET /api/agents - List agents');
    console.log('  POST /api/agents - Create agent');
    console.log('  GET /api/calls - List calls');
});