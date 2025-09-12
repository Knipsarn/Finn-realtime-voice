/**
 * SQLite Database Module
 * Handles agent and call data storage for Twilio GPT-Realtime integration
 */

import sqlite3 from 'sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

class Database {
    constructor() {
        this.db = null;
        this.dbPath = path.join(__dirname, 'twilio_gpt.db');
    }

    async initialize() {
        return new Promise((resolve, reject) => {
            this.db = new sqlite3.Database(this.dbPath, (err) => {
                if (err) {
                    console.error('Database connection error:', err.message);
                    reject(err);
                    return;
                }
                console.log('Connected to SQLite database');
                this.createTables().then(resolve).catch(reject);
            });
        });
    }

    async createTables() {
        const queries = [
            `CREATE TABLE IF NOT EXISTS agents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                voice TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,
            `CREATE TABLE IF NOT EXISTS calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id INTEGER REFERENCES agents(id),
                from_number TEXT NOT NULL,
                to_number TEXT NOT NULL,
                started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                ended_at DATETIME,
                transcript TEXT,
                latency_ms INTEGER
            )`
        ];

        for (const query of queries) {
            await this.run(query);
        }

        await this.createDefaultAgent();
    }

    async createDefaultAgent() {
        const existingAgent = await this.get('SELECT id FROM agents LIMIT 1');
        if (!existingAgent) {
            await this.run(
                'INSERT INTO agents (prompt, voice) VALUES (?, ?)',
                [
                    'Du är en hjälpsam AI-assistent som talar svenska. Svara naturligt och använd svenska konversationsmönster. Var kortfattad om inte användaren ber om detaljerad information.',
                    'alloy'
                ]
            );
            console.log('Created default Swedish agent');
        }
    }

    run(query, params = []) {
        return new Promise((resolve, reject) => {
            this.db.run(query, params, function(err) {
                if (err) reject(err);
                else resolve({ id: this.lastID, changes: this.changes });
            });
        });
    }

    get(query, params = []) {
        return new Promise((resolve, reject) => {
            this.db.get(query, params, (err, row) => {
                if (err) reject(err);
                else resolve(row);
            });
        });
    }

    all(query, params = []) {
        return new Promise((resolve, reject) => {
            this.db.all(query, params, (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });
    }

    async createAgent(prompt, voice) {
        const validVoices = ['alloy', 'echo', 'fable', 'onyx', 'nova', 'shimmer', 'cedar', 'marin'];
        if (!validVoices.includes(voice)) {
            throw new Error(`Invalid voice. Must be one of: ${validVoices.join(', ')}`);
        }

        const result = await this.run(
            'INSERT INTO agents (prompt, voice) VALUES (?, ?)',
            [prompt, voice]
        );
        return result.id;
    }

    async getAgent(id) {
        return await this.get('SELECT * FROM agents WHERE id = ?', [id]);
    }

    async getAllAgents() {
        return await this.all('SELECT * FROM agents ORDER BY created_at DESC');
    }

    async updateAgent(id, prompt, voice) {
        const validVoices = ['alloy', 'echo', 'fable', 'onyx', 'nova', 'shimmer', 'cedar', 'marin'];
        if (!validVoices.includes(voice)) {
            throw new Error(`Invalid voice. Must be one of: ${validVoices.join(', ')}`);
        }

        const result = await this.run(
            'UPDATE agents SET prompt = ?, voice = ? WHERE id = ?',
            [prompt, voice, id]
        );
        return result.changes > 0;
    }

    async createCall(agentId, fromNumber, toNumber) {
        const result = await this.run(
            'INSERT INTO calls (agent_id, from_number, to_number) VALUES (?, ?, ?)',
            [agentId, fromNumber, toNumber]
        );
        return result.id;
    }

    async endCall(callId, transcript = null, latencyMs = null) {
        const result = await this.run(
            'UPDATE calls SET ended_at = CURRENT_TIMESTAMP, transcript = ?, latency_ms = ? WHERE id = ?',
            [transcript, latencyMs, callId]
        );
        return result.changes > 0;
    }

    async getCall(id) {
        return await this.get(`
            SELECT c.*, a.prompt, a.voice 
            FROM calls c 
            JOIN agents a ON c.agent_id = a.id 
            WHERE c.id = ?
        `, [id]);
    }

    async getRecentCalls(limit = 50) {
        return await this.all(`
            SELECT c.*, a.prompt, a.voice 
            FROM calls c 
            JOIN agents a ON c.agent_id = a.id 
            ORDER BY c.started_at DESC 
            LIMIT ?
        `, [limit]);
    }

    close() {
        if (this.db) {
            this.db.close((err) => {
                if (err) {
                    console.error('Error closing database:', err.message);
                } else {
                    console.log('Database connection closed');
                }
            });
        }
    }
}

export default new Database();