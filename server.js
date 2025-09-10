#!/usr/bin/env node
/**
 * GPT-Realtime Voice Test Harness Server
 * Provides endpoints for session management and static file serving
 */

import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import OpenAI from 'openai';
import 'dotenv/config';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3010;

// Initialize OpenAI client
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY
});

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

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ 
        status: 'ok', 
        service: 'gpt-realtime-voice-test-harness',
        openai_configured: !!process.env.OPENAI_API_KEY,
        timestamp: new Date().toISOString()
    });
});

// Start server
app.listen(PORT, () => {
    console.log(`GPT-Realtime Voice Test Harness running on http://localhost:${PORT}`);
    console.log('Static client available at GET /');
    console.log('API endpoints:');
    console.log('  GET /health - Service health check');
    console.log('  GET /api/config - Configuration presets (pending T015)');
    console.log('  POST /api/session - Session creation (pending T016)');
});