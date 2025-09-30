import { createClient } from "@clickhouse/client";

const client = createClient({
  url: process.env.CLICKHOUSE_URL!,
  username: process.env.CLICKHOUSE_USERNAME!,
  password: process.env.CLICKHOUSE_PASSWORD!,
});


const server: Bun.Server = Bun.serve({
  port: 3000,
  async fetch(req) {
    const url = new URL(req.url);

    if (req.method === "GET" && url.pathname === "/health") {
      return new Response("OK", { status: 200 });
    }

    if (req.method === "GET" && url.pathname === "/transfers") {
      // Extract query parameters
      const timeInterval = url.searchParams.get("timeInterval");
      const token = url.searchParams.get("token");
      const toBlock = url.searchParams.get("toBlock");
      
      console.log("Extracted parameters:", { timeInterval, tokenPair: token, toBlock });
      
      // Validate required parameters
      if (!timeInterval || !token || !toBlock) {
        return new Response(
          JSON.stringify({
            error: "Missing required parameters",
            required: ["timeInterval", "tokenPair", "toBlock"],
            provided: { timeInterval, tokenPair: token, toBlock }
          }),
          { 
            status: 400,
            headers: { "Content-Type": "application/json" }
          }
        );
      }
        
      try {

        if(token !== "AAVE") {
          return new Response(
            JSON.stringify({
              error: "Invalid token",
              valid: ["AAVE"]
            }),
            { status: 400, headers: { "Content-Type": "application/json" } }
          );
        }
        

        let table = "aave_transfers";
        
        if(timeInterval === "1h") { 
          table = "aave_1h_transfers";
        }
        if(timeInterval === "3h") { 
          table = "aave_3h_transfers";
        }
        
        console.time("clickhouse_query");

        // Build dynamic query based on parameters
        const query = `
          SELECT 
            time_bucket, countMerge(total_transfers) as total_transfers, 
            sumMerge(total_amount) as total_amount, 
            avgMerge(avg_amount) as avg_amount, 
            minMerge(min_amount) as min_amount, 
            maxMerge(max_amount) as max_amount 
          FROM "${table}" 
          GROUP BY time_bucket
          ORDER BY time_bucket DESC;
        `;
        
        // console.log("Executing query:", query);
        
        const rows = await client.query({ query, format: "JSONEachRow" });
        console.timeLog("clickhouse_query", "Query executed");

        const data = await rows.json();
        console.timeEnd("clickhouse_query");
        
        // Return data with metadata
        return new Response(JSON.stringify({
          parameters: {
            timeInterval,
            tokenPair: token,
            toBlock: parseInt(toBlock)
          },
          data,
          count: data.length
        }), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (error) {
        console.error("Query error:", error);
        return new Response(
          JSON.stringify({
            error: "Query failed",
            message: error instanceof Error ? error.message : "Unknown error"
          }),
          { 
            status: 500,
            headers: { "Content-Type": "application/json" }
          }
        );
      }
    }

    return new Response("Not Found", { status: 404 });
  },
});

console.log(`Server running on http://localhost:${server.port}`);
console.log(`Query: http://localhost:${server.port}/transfers`);
console.log(`Health: http://localhost:${server.port}/health`);
console.log(`WebSocket: ws://localhost:${server.port}`);
