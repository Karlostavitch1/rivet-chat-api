// src/graphManager.ts

import * as Rivet from '@ironclad/rivet-node';
import fs from 'fs/promises';
import path from 'path';
import { setupPlugins, logAvailablePluginsInfo } from './pluginConfiguration.js';
import { delay } from './utils.js';
import event from 'events';
import { QdrantDatasetProvider } from './QdrantDatasetProvider.js';
import yaml from 'yaml';
import dotenv from 'dotenv';
dotenv.config();

logAvailablePluginsInfo();
event.setMaxListeners(100);

class DebuggerServer {
  private static instance: DebuggerServer | null = null;
  private debuggerServer: any = null;

  private constructor() {}

  public static getInstance(): DebuggerServer {
    if (!DebuggerServer.instance) {
      DebuggerServer.instance = new DebuggerServer();
    }
    return DebuggerServer.instance;
  }

  public startDebuggerServerIfNeeded() {
    if (!this.debuggerServer) {
      this.debuggerServer = Rivet.startDebuggerServer();
      console.log('Debugger server started');
    }
    return this.debuggerServer;
  }

  public getDebuggerServer() {
    return this.debuggerServer;
  }
}

export class GraphManager {
  config: any;
  modelContent?: string;
  streamedNodeIds: Set<string>;
  private static datasetProvider: QdrantDatasetProvider | null = null;
  streamingOutputNodeName: string;

  constructor(params: { config?: any; modelContent?: string }) {
    this.config = params.config || {};
    this.modelContent = params.modelContent;
    this.streamedNodeIds = new Set();
  }
  

  // Function to check if a string is a valid UUID
  private isValidUUID(id: string): boolean {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    return uuidRegex.test(id);
  }

  // Initialize the DatasetProvider
  private static async initializeDatasetProvider() {
    if (!GraphManager.datasetProvider) {
      console.log('Initializing QdrantDatasetProvider...');
      try {
        GraphManager.datasetProvider = await QdrantDatasetProvider.create();
        console.log('QdrantDatasetProvider initialized successfully.');
      } catch (error) {
        console.error('Failed to initialize QdrantDatasetProvider:', error);
        throw error;
      }
    }
  }

  // Debug helper function to log the project content and ID extraction
  private logProjectIdDetails(projectContent: string, projectId?: string) {
    console.log("-----------------------------------------------------");
    console.log('Project Content (truncated):', projectContent.slice(0, 200), '...');
    if (projectId) {
      console.log(`Extracted projectId: ${projectId}`);
    } else {
      console.log('No projectId found in the project content.');
    }
    console.log("-----------------------------------------------------");
  }

  async *runGraph(
    messages: Array<{ type: 'user' | 'assistant'; message: string }>,
    user: string
  ) {
    console.time('runGraph');
    let projectContent: string;
  
    // Ensure the DebuggerServer is started
    DebuggerServer.getInstance().startDebuggerServerIfNeeded();
  
    try {
      const pluginSettings = await setupPlugins(Rivet);
  
      if (this.modelContent) {
        // Use the provided model content if available
        projectContent = this.modelContent;
      } else {
        // Otherwise, load the model file from the filesystem using the config
        const modelFilePath = path.resolve(process.cwd(), './rivet', this.config.file);
        console.log('runGraph called with model file:', modelFilePath);
        projectContent = await fs.readFile(modelFilePath, 'utf8');
      }

  
      // Parse the YAML content into an object
      const parsedContent = yaml.parse(projectContent);
  
      // Navigate to the `metadata` section under `data`
      let projectId: string | undefined;
      if (
        parsedContent.data &&
        parsedContent.data.metadata &&
        parsedContent.data.metadata.id
      ) {
        projectId = parsedContent.data.metadata.id;
        console.log(`Extracted projectId from data -> metadata: ${projectId}`);
      } else {
        console.error(
          "Error: Could not find a valid projectId in the data -> metadata section."
        );
        throw new Error(
          "Invalid projectId: No projectId found in the data -> metadata section."
        );
      }
  
      // Pass the entire project content to createProcessor
      const project = Rivet.loadProjectFromString(projectContent);
  
      // Initialize the DatasetProvider
      await GraphManager.initializeDatasetProvider();
  
      // Proceed with the rest of the graph execution logic
      const graphInput = this.config.graphInputName || "input";
      const userInput = this.config.userInputName || "user";
  
      const options: Rivet.NodeRunGraphOptions = {
        graph: this.config.graphName,
        inputs: {
          [graphInput]: {
            type: 'chat-message[]',
            value: messages.map(
              (message) => ({
                type: message.type,
                message: message.message,
              } as Rivet.ChatMessage)
            ),
          },
          [userInput]: {
            type: 'string',
            value: user,
          }
        },
        openAiKey: process.env.OPENAI_API_KEY,
        remoteDebugger: DebuggerServer.getInstance().getDebuggerServer(),
        datasetProvider: GraphManager.datasetProvider,
        pluginSettings,
        context: {
          ...Object.entries(process.env).reduce((acc, [key, value]) => {
            acc[key] = value!;
            return acc;
          }, {} as Record<string, string>),
        },
        onUserEvent: {
          debugger: (data: Rivet.DataValue): Promise<void> => {
            return Promise.resolve();
          }
        }
      };
  
      console.log('Creating processor');
      const { processor, run } = Rivet.createProcessor(project, options);
      const runPromise = run();
      console.log('Starting to process events');
  
      let lastContent = '';
  
      // Define the type for the event
      type Event = {
        type: string;
        node?: {
          title?: string;
          id?: string;
          type?: string;
        };
        outputs?: {
          response?: { value: string };
          output?: { value: string; output: string };
        };
      };
  
      for await (const event of processor.events() as AsyncIterable<Event>) {
        // Filter and log only events related to the node 'Output (Chat)'
        if ('node' in event && event.node?.title === 'Output (Chat)') {
          if (event.type === 'partialOutput') {
            const content = event.outputs?.response?.value || event.outputs?.output?.value;
            if (content && content.startsWith(lastContent)) {
              const delta = content.slice(lastContent.length);
              yield delta;
              lastContent = content;
              this.streamedNodeIds.add(event.node.id); // Add node ID to the Set when streaming output
            }
          } else if (
            event.type === 'nodeFinish' &&
            !event.node?.type?.includes('chat') &&
            !this.streamedNodeIds.has(event.node.id) // Check if the node ID is not in the streamedNodeIds Set
          ) {
            try {
              let content = event.outputs?.output?.value || event.outputs?.output?.output;
              if (content) {
                if (typeof content !== 'string') {
                  content = JSON.stringify(content);
                }
                for (const char of content) {
                  await delay(0.5); // Artificial delay to simulate streaming
                  yield char;
                }
              }
            } catch (error) {
              console.error(`Error: Cannot return output from node of type ${event.node?.type}. This only works with certain nodes (e.g., text or object)`);
            }
          }
        }
      }
  
      console.log('Finished processing events');
  
      const finalOutputs = await runPromise;
      if (finalOutputs && finalOutputs["output"]) {
        yield finalOutputs["output"].value;
      }
      if (finalOutputs["cost"]) {
        console.log(`Cost: ${finalOutputs["cost"].value}`);
      }
    } catch (error) {
      console.error('Error in runGraph:', error);
    } finally {
      console.timeEnd('runGraph');
    }
  }
}
