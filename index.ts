import { createClient } from "@clickhouse/client";

const client = createClient({
  url: process.env.CLICKHOUSE_URL!,
  username: process.env.CLICKHOUSE_USERNAME!,
  password: process.env.CLICKHOUSE_PASSWORD!,
});


// Helper to add CORS headers to any response
const cors = (response: Response) => {
  response.headers.set("Access-Control-Allow-Origin", "*");
  response.headers.set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  response.headers.set("Access-Control-Allow-Headers", "Content-Type");
  return response;
};

const server: Bun.Server = Bun.serve({
  port: 3000,
  async fetch(req) {
    const url = new URL(req.url);

    // Handle preflight OPTIONS requests
    if (req.method === "OPTIONS") {
      return cors(new Response(null, { status: 204 }));
    }

    if (req.method === "GET" && url.pathname === "/health") {
      return cors(new Response("OK", { status: 200 }));
    }

    if (req.method === "GET" && url.pathname === "/transfers") {
      // Extract query parameters
      const timeInterval = url.searchParams.get("timeInterval");
      const token = url.searchParams.get("token");
      const toBlock = url.searchParams.get("toBlock");
      const fromTimestamp = url.searchParams.get("fromTimestamp"); // in seconds
      const toTimestamp = url.searchParams.get("toTimestamp"); // in seconds
      
      console.log("Extracted parameters:", { timeInterval, token, toBlock, fromTimestamp, toTimestamp });
      
      // Validate required parameters
      if (!timeInterval || !token) {  
        return cors(new Response(
          JSON.stringify({
            error: "Missing required parameters",
            required: ["timeInterval", "token"],
            optional: ["toBlock", "fromTimestamp", "toTimestamp"],
            provided: { timeInterval, token, toBlock, fromTimestamp, toTimestamp }
          }),
          { 
            status: 400,
            headers: { "Content-Type": "application/json" }
          }
        ));
      }
        
      try {

        if(token !== "AAVE") {
          return cors(new Response(
            JSON.stringify({ error: "Invalid token", valid: ["AAVE"] }),
            { status: 400, headers: { "Content-Type": "application/json" } }
          ));
        }
        

        let table = null;
        
        if(timeInterval === "1h") { 
          table = "aave_1h_transfers";
        }
        if(timeInterval === "1m") { 
          table = "aave_1m_transfers";
        }

        if(!table) {
          return cors(new Response(
            JSON.stringify({ error: "Invalid time interval", valid: ["1h", "1m"] }),
            { status: 400, headers: { "Content-Type": "application/json" } }
          ));
        }
        
        console.time("clickhouse_query");

        const query = `
          SELECT 
            time_bucket, 
            countMerge(total_transfers) as total_transfers, 
            sumMerge(total_amount) as total_amount, 
            avgMerge(avg_amount) as avg_amount, 
            minMerge(min_amount) as min_amount, 
            maxMerge(max_amount) as max_amount,
            argMinMerge(open_amount) as open_amount,
            argMaxMerge(close_amount) as close_amount,
            argMaxMerge(last_block_number) as last_block_number,
            quantileMerge(0.5)(median_amount) as median_amount
          FROM "${table}"
          WHERE 1=1
          ${fromTimestamp ? `AND time_bucket >= toDateTime(${fromTimestamp})` : ""}
          ${toTimestamp ? `AND time_bucket <= toDateTime(${toTimestamp})` : ""}
          GROUP BY time_bucket
          ${toBlock ? `HAVING last_block_number <= ${+toBlock}` : ""}
          ORDER BY last_block_number DESC;
        `;

        console.log("Executing query:", query);
        

        const maxRetries = 10; // Maximum number of retry attempts
        const retryDelay = 250; // Delay between retries in milliseconds
        let attempt = 0;
        let data: any = null;
        
        while (attempt < maxRetries) {
          attempt++;
          console.log(`Query attempt ${attempt}/${maxRetries}`);
          
          const rows = await client.query({ query });
          const result = await rows.json();
          
          if (result.rows && result.rows > 0) {
            console.timeLog("clickhouse_query", `Query succeeded on attempt ${attempt}`);
            data = result;
            break;
          }

          if (attempt >= maxRetries) {
            console.timeLog("clickhouse_query", `Max retries reached (${maxRetries})`);
            data = result;
            break;
          }
          
          console.log(`No data found, retrying in ${retryDelay}ms...`);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
        }
        
        console.timeEnd("clickhouse_query");
        
        return cors(new Response(JSON.stringify({
          parameters: {
            timeInterval,
            tokenPair: token,
            toBlock: toBlock ? parseInt(toBlock) : null,
            fromTimestamp: fromTimestamp ? parseInt(fromTimestamp) : null,
            toTimestamp: toTimestamp ? parseInt(toTimestamp) : null,
          },
          data: data.data,
          count: data.rows
        }), {
          headers: { "Content-Type": "application/json" },
        }));
      } catch (error) {
        console.error("Query error:", error);
        return cors(new Response(
          JSON.stringify({
            error: "Query failed",
            message: error instanceof Error ? error.message : "Unknown error"
          }),
          { 
            status: 500,
            headers: { "Content-Type": "application/json" }
          }
        ));
      }
    }

    return cors(new Response("Not Found", { status: 404 }));
  },
});

console.log(`Server running on http://localhost:${server.port}`);
console.log(`Query: http://localhost:${server.port}/transfers`);
console.log(`Health: http://localhost:${server.port}/health`);
console.log(`WebSocket: ws://localhost:${server.port}`);
