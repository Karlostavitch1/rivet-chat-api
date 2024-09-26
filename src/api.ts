import express from 'express';
import config from 'config'; // Import the config library
import { GraphManager } from './graphManager.js';
import morgan from 'morgan';
import cors from 'cors';
import dotenv from 'dotenv';
dotenv.config();


const app = express();
const port = process.env.PORT || 3100;
const environment = process.env.NODE_ENV;
const apiKey = process.env.RIVET_CHAT_API_KEY;

app.use(express.json());
app.use(morgan('combined'));

app.use(cors({
    origin: process.env.ACCESS_CONTROL_ALLOW_ORIGIN || '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
}));

// Middleware for API Key validation
app.use((req, res, next) => {
    if (environment === 'production') {
        const authHeader = req.headers.authorization;
        if (!req.hostname.endsWith('.internal')) {
            if (!authHeader || authHeader !== `Bearer ${apiKey}`) {
                return res.status(403).json({ message: 'Forbidden - Invalid API Key' });
            }
        }
    }
    next();
});

// POST endpoint for chat completions
app.post('/chat/completions', async (req, res) => {
    const modelId = req.body.model;
    const messages = req.body.messages;
    const user = req.body.user;
  
    if (!modelId || !messages || !Array.isArray(messages) || !user) {
      return res.status(400).json({ message: 'Invalid input data' });
    }
  
    const processedMessages = messages.map(({ role: type, content: message }) => ({ type, message }));
  
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Transfer-Encoding', 'chunked');
  
    // Load configuration based on the model
    const servers: { file: string }[] = config.get('servers');
    const serverConfig = servers.find(server => server.file === modelId);
    if (!serverConfig) {
      return res.status(404).json({ message: 'Model not found in configuration' });
    }
  
    const commonData = {
      id: 'chatcmpl-mockId12345',
      object: 'chat.completion.chunk',
      created: Date.now(),
      model: modelId,
      system_fingerprint: null,
    };
  
    // Function to process and send chunks
    async function processAndSendChunks(graphManager) {
      let isFirstChunk = true;
      let previousChunk = null;
      let accumulatedContent = "";
  
      for await (const chunk of graphManager.runGraph(processedMessages, user)) {
        console.log('Chunk received:', chunk); // Debug log
  
        if (isFirstChunk) {
          isFirstChunk = false;
          previousChunk = { role: "assistant", content: chunk };
        } else {
          if (previousChunk !== null) {
            const chunkData = {
              ...commonData,
              choices: [{ index: 0, delta: previousChunk, logprobs: null, finish_reason: null }],
            };
            res.write(`data: ${JSON.stringify(chunkData)}\n\n`);
            accumulatedContent += previousChunk.content;
          }
          previousChunk = { content: chunk };
        }
      }
  
      if (previousChunk !== null) {
        accumulatedContent += previousChunk.content;
        const lastChunkData = {
          ...commonData,
          choices: [{ index: 0, delta: previousChunk, logprobs: null, finish_reason: "stop" }],
        };
        res.write(`data: ${JSON.stringify(lastChunkData)}\n\n`);
      }
  
      res.write('data: [DONE]\n\n');
      res.end();
    }
  
    const graphManager = new GraphManager({ config: serverConfig });
    await processAndSendChunks(graphManager);
  });

// Listen on the configured port
app.listen(Number(port), () => {
    console.log(`Server running on port ${port}`);
});