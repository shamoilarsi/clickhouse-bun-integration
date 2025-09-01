import { createClient } from '@clickhouse/client'

const client = createClient({
    url: process.env.CLICKHOUSE_URL!,
    username: process.env.CLICKHOUSE_USERNAME!,
    password: process.env.CLICKHOUSE_PASSWORD!,
})

const connectedClients = new Set<Bun.ServerWebSocket<unknown>>()

function broadcastToClients(message: any) {
    const messageString = JSON.stringify(message)
    console.log(`Broadcasting to ${connectedClients.size} clients:`, message)
    
    // Remove closed connections and send to active ones
    for (const client of connectedClients) {
        if (client.readyState === 1) { // WebSocket.OPEN
            try {
                client.send(messageString)
            } catch (error) {
                console.error('Error sending message to client:', error)
                connectedClients.delete(client)
            }
        } else {
            connectedClients.delete(client)
        }
    }
}


function validateWebhookSecret(request: Request): boolean {
    const secret = request.headers.get('Webhook-Auth-Key')
    const expectedSecret = process.env.GOLDSKY_WEBHOOK_SECRET

    if (!expectedSecret) {
                            console.warn('GOLDSKY_WEBHOOK_SECRET not set')
        return false
    }
    
    return secret === expectedSecret
}

const server = Bun.serve({
    port: 3000,
    websocket: {
        message(ws, message) {
            console.log('Received WebSocket message:', message)
            ws.send(`Echo: ${message}`)
        },
        open(ws) {
            console.log('WebSocket client connected')
            connectedClients.add(ws)

            ws.send(JSON.stringify({
                type: 'connection_established',
                timestamp: new Date().toISOString(),
                message: 'Connected to Goldsky webhook stream'
            }))
        },
        close(ws) {
            console.log('WebSocket client disconnected')
            connectedClients.delete(ws)
        },
        drain(ws) {
            console.log('WebSocket backpressure drained')
        },
    },
    async fetch(req) {
        const url = new URL(req.url)
        
        // WebSocket upgrade endpoint
        if (req.method === 'GET' && url.pathname === '/ws') {
            const success = server.upgrade(req)
            if (success) {
                return undefined // Do not return a Response
            } else {
                return new Response('WebSocket upgrade failed', { status: 400 })
            }
        }
        
        // Goldsky webhook endpoint
        if (req.method === 'POST' && url.pathname === '/webhook/goldsky') {          
            try {
                // Validate webhook secret
                if (!validateWebhookSecret(req)) {
                    console.warn('Invalid webhook secret')
                    return new Response('Unauthorized', { status: 401 })
                }
                
                // Parse the webhook payload
                const event = await req.json() 
                
                // Process the event
                console.log('Received webhook event:', event)
                
                // Broadcast the event to all connected WebSocket clients
                broadcastToClients({
                    type: 'goldsky_webhook',
                    timestamp: new Date().toISOString(),
                    data: event
                })
                
                return new Response('OK', { status: 200 })
            } catch (error) {
                console.error('Error processing webhook:', error)
                return new Response('Internal Server Error', { status: 500 })
            }
        }
        
        // Health check endpoint
        if (req.method === 'GET' && url.pathname === '/health') {
            return new Response('OK', { status: 200 })
        }
        
        // WebSocket test page
        if (req.method === 'GET' && url.pathname === '/test') {
            const html = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WebSocket Test - Goldsky Webhook Stream</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .status { padding: 10px; border-radius: 4px; margin: 10px 0; }
        .connected { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .disconnected { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .message { background: #e7f3ff; border: 1px solid #b8daff; padding: 10px; margin: 5px 0; border-radius: 4px; }
        .controls { margin: 20px 0; }
        button { background: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; margin: 5px; }
        button:hover { background: #0056b3; }
        button:disabled { background: #6c757d; cursor: not-allowed; }
        #messages { max-height: 400px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; background: #f8f9fa; }
        .timestamp { color: #6c757d; font-size: 0.9em; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔌 WebSocket Test - Goldsky Webhook Stream</h1>
        
        <div id="status" class="status disconnected">
            Disconnected
        </div>
        
        <div class="controls">
            <button id="connectBtn">Connect</button>
            <button id="disconnectBtn" disabled>Disconnect</button>
            <button id="clearBtn">Clear Messages</button>
        </div>
        
        <h3>Messages:</h3>
        <div id="messages"></div>
    </div>

    <script>
        let ws = null;
        const status = document.getElementById('status');
        const messages = document.getElementById('messages');
        const connectBtn = document.getElementById('connectBtn');
        const disconnectBtn = document.getElementById('disconnectBtn');
        const clearBtn = document.getElementById('clearBtn');
        
        function updateStatus(connected) {
            if (connected) {
                status.textContent = 'Connected to WebSocket';
                status.className = 'status connected';
                connectBtn.disabled = true;
                disconnectBtn.disabled = false;
            } else {
                status.textContent = 'Disconnected';
                status.className = 'status disconnected';
                connectBtn.disabled = false;
                disconnectBtn.disabled = true;
            }
        }
        
        function addMessage(data) {
            const messageDiv = document.createElement('div');
            messageDiv.className = 'message';
            const timestamp = new Date().toLocaleTimeString();
            messageDiv.innerHTML = \`
                <div class="timestamp">\${timestamp}</div>
                <pre>\${JSON.stringify(data, null, 2)}</pre>
            \`;
            messages.appendChild(messageDiv);
            messages.scrollTop = messages.scrollHeight;
        }
        
        function connect() {
            const wsUrl = \`ws://\${window.location.host}/ws\`;
            ws = new WebSocket(wsUrl);
            
            ws.onopen = function(event) {
                updateStatus(true);
                addMessage({ type: 'system', message: 'WebSocket connection opened' });
            };
            
            ws.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    addMessage(data);
                } catch (e) {
                    addMessage({ type: 'raw', message: event.data });
                }
            };
            
            ws.onclose = function(event) {
                updateStatus(false);
                addMessage({ type: 'system', message: 'WebSocket connection closed', code: event.code });
            };
            
            ws.onerror = function(error) {
                addMessage({ type: 'error', message: 'WebSocket error occurred' });
            };
        }
        
        function disconnect() {
            if (ws) {
                ws.close();
                ws = null;
            }
        }
        
        connectBtn.addEventListener('click', connect);
        disconnectBtn.addEventListener('click', disconnect);
        clearBtn.addEventListener('click', () => {
            messages.innerHTML = '';
        });
        
        // Auto-connect on page load
        connect();
    </script>
</body>
</html>
            `;
            return new Response(html, {
                headers: { 'Content-Type': 'text/html' }
            })
        }
        
        // Original ClickHouse query endpoint (for testing)
        if (req.method === 'GET' && url.pathname === '/query') {
            try {
                console.time('query')
                const rows = await client.query({
                    query: 'SELECT * FROM "usdc_raw_transfers" LIMIT 31 OFFSET 0;',
                    format: 'JSONEachRow',
                })
                
                console.timeLog('query', 'Query executed')
                const data = await rows.json()
                console.timeEnd('query')
                return new Response(JSON.stringify(data), {
                    headers: { 'Content-Type': 'application/json' }
                })
            } catch (error) {
                console.error('Query error:', error)
                return new Response('Query failed', { status: 500 })
            }
        }
        
        return new Response('Not Found', { status: 404 })
    },
})

console.log(`Server running on http://localhost:${server.port}`)
console.log(`Webhook: http://localhost:${server.port}/webhook/goldsky`)
console.log(`WebSocket: ws://localhost:${server.port}/ws`)
console.log(`Test page: http://localhost:${server.port}/test`)
console.log(`Query: http://localhost:${server.port}/query`)
console.log(`Health: http://localhost:${server.port}/health`)
  