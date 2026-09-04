import { Hono } from "https://esm.sh/hono@4.12.27";
import { cors } from "https://esm.sh/hono@4.12.27/cors";
import { handle } from "https://esm.sh/hono@4.12.27/netlify";
import { streamSSE } from 'https://esm.sh/hono@4.12.27/streaming';
import YahooFinance from "https://esm.sh/yahoo-finance2";
const instanceId = crypto.randomUUID();
const MAX_REQUESTS_PER_INVOCATION = 50;

// Start a Hono app
const app = new Hono();

app.use('/api/*', cors());

app.use("*", async (c, next) => {
    c.header("x-server-instance-id", instanceId);
    await next();
});

app.onError((err, c) => {
    console.error("Global error handler caught:", (err as Error).message); // Log the error if it's not known

    // For other errors, return a generic 500 response
    return c.json(
        {
            success: false,
            errors: [{ code: 7000, message: "Internal Server Error" }],
        },
        500,
    );
});

const yf = new YahooFinance({
    suppressNotices: ["yahooSurvey"], // optional
});

app.get('/api/hello', (c) => {
    return c.json({
        "message": "hello"
    });
})

app.get("/api/live-quotes", (c) => {
    const normalizedSymbol = new Set(c.req.query("s")?.split(',').map(s => s.trim().toUpperCase()).filter(Boolean) || ['AAPL']);
    const interval = c.req.query("i") ? parseInt(c.req.query("i")!) : 1000;
    let requestCount = 0;
    return streamSSE(c, async (stream) => {
        let anySuccessfulFetch = false;
        const writePrice = async (symbol: string) => {
            try {
                const priceData = await fetchPrice(symbol);
                priceData.change = Math.round(priceData.change * 100) / 100;
                await stream.writeSSE({
                    event: 'quote',
                    data: JSON.stringify({
                        t: Date.now(),
                        symbol,
                        ...priceData,
                        changePercent: Math.round((priceData.change / priceData.price * 100) * 100) / 100
                    }),
                })
                anySuccessfulFetch = true;
            } catch (error) {
                console.error(`Error fetching price for ${symbol}:`, (error as Error).message);
            }
        }
        while (!stream.aborted && !stream.closed && requestCount++ < MAX_REQUESTS_PER_INVOCATION) {
            anySuccessfulFetch = false;	//reset flag
            await Promise.allSettled([...normalizedSymbol.values().map(writePrice), stream.sleep(interval)]);
            if (!anySuccessfulFetch) {
                console.error("All price fetches failed");
                stream.abort();
            }
        }
    })
});

export async function fetchPrice(symbol: string) {
    const fields = ["marketState",
        "regularMarketPrice",
        "regularMarketChange",
        "postMarketPrice",
        "postMarketChange",
        "preMarketPrice",
        "preMarketChange"];
    const {
        marketState,
        regularMarketPrice,
        regularMarketChange,
        postMarketPrice,
        postMarketChange,
        preMarketPrice,
        preMarketChange,
    } = await yf.quoteCombine(symbol === 'SPX' ? '^SPX' : symbol, { fields }, {
        validateResult: false
    }) as any;

    // Default null/undefined values to 0
    const preMarketChangeVal = preMarketChange ?? 0;
    const regularMarketChangeVal = regularMarketChange ?? 0;
    const postMarketChangeVal = postMarketChange ?? 0;

    switch (marketState) {
        case "PRE":
            return {
                price: preMarketPrice || regularMarketPrice,
                change: preMarketChangeVal,
                state: "PRE",
                meta: {
                    preMarketChange: preMarketChangeVal,
                    regularMarketChange: regularMarketChangeVal,
                    postMarketChange: postMarketChangeVal
                }
            };
        case "REGULAR":
            return {
                price: regularMarketPrice,
                change: regularMarketChangeVal,
                state: "REGULAR",
                meta: {
                    preMarketChange: preMarketChangeVal,
                    regularMarketChange: regularMarketChangeVal,
                    postMarketChange: postMarketChangeVal
                }
            };
        default:
            return {
                price: postMarketPrice || regularMarketPrice,
                change: regularMarketChangeVal + postMarketChangeVal,
                state: "POST",
                meta: {
                    preMarketChange: preMarketChangeVal,
                    regularMarketChange: regularMarketChangeVal,
                    postMarketChange: postMarketChangeVal
                }
            };
    }
}

console.info(`App started with instance: ${instanceId}`)

export const config = { path: "/api/*" };
export default handle(app);