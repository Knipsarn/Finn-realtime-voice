# gpt-realtime-test2 Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-09-08

## Active Technologies
- Node.js 18+ + Express.js (001-real-time-audio)
- OpenAI Realtime API + WebRTC APIs (001-real-time-audio)
- Vanilla HTML/CSS/JavaScript ES2020+ (001-real-time-audio)

## Project Structure
```
server/
├── index.js             # Express app with /session endpoint and static hosting
└── package.json

public/
├── index.html           # Single page UI with controls and audio elements
├── app.js              # WebRTC logic, OpenAI signaling, UI wiring
└── style.css           # Minimal styling

.env.example            # Environment template
README.md              # Quick start and troubleshooting
package.json           # Root dependencies and npm run dev script
```

## Commands
# Development server
npm run dev

# Start server
npm start

## Code Style
- Keep server under 300 lines of code
- Keep client under 350 lines of code
- Use vanilla JavaScript (no frameworks)
- Follow constitutional simplicity principles

## Recent Changes
- 001-real-time-audio: Added OpenAI Realtime API + WebRTC + Swedish voice testing

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->