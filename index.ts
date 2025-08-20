import { createClient } from '@clickhouse/client'

const client = createClient({
    url: process.env.CLICKHOUSE_URL!,
    username: process.env.CLICKHOUSE_USERNAME!,
    password: process.env.CLICKHOUSE_PASSWORD!,
  })


const server = Bun.serve({
    port: 3000,
    async fetch(req) {
        console.time('query')
          const rows = await client.query({
            query: 'SELECT * FROM "usdc_raw_transfers" LIMIT 31 OFFSET 0;',
            format: 'JSONEachRow',
          })
          console.timeLog('query', 'Query executed')
          const data = await rows.json()
          console.timeEnd('query')
          return new Response(JSON.stringify(data));
    },
  });
  
  console.log(`Listening on http://localhost:${server.port} ...`);
  