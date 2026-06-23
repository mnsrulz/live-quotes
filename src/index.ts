import { ApiException } from "chanfana";
import { Hono } from "hono";
import { ContentfulStatusCode } from "hono/utils/http-status";
import { streamSSE } from 'hono/streaming';
import YahooFinance from "yahoo-finance2";

// Start a Hono app
const app = new Hono();

app.onError((err, c) => {
	if (err instanceof ApiException) {
		// If it's a Chanfana ApiException, let Chanfana handle the response
		return c.json(
			{ success: false, errors: err.buildResponse() },
			err.status as ContentfulStatusCode,
		);
	}

	console.error("Global error handler caught:", err); // Log the error if it's not known

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

app.get('/', (c) => {
	return c.redirect('/index.htm');
})

app.get("/live-quotes", (c) => {
	const normalizedSymbol = c.req.query("s")?.split(',').map(s => s.trim().toUpperCase()) || ['AAPL'];
	const interval = c.req.query("i") ? parseInt(c.req.query("i")!) : 1000;
	return streamSSE(c, async (stream) => {
		const writePrice = async (symbol: string) => {
			try {
				const priceData = await fetchPrice(symbol);
				priceData.change = priceData.change.toFixed(2);
				await stream.writeSSE({
					event: 'quote',
					data: JSON.stringify({
						t: Date.now(),
						symbol,
						...priceData,
						changePercent: (priceData.change / priceData.price * 100).toFixed(2)
					}),
				})
			} catch (error) {
				console.error(`Error fetching price for ${symbol}:`, error);
			}
		}
		while (!stream.aborted && !stream.closed) {
			await Promise.allSettled([...normalizedSymbol.map(writePrice), stream.sleep(interval)]);
		}
	})
});

async function fetchPrice(symbol: string) {
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
	} = await yf.quoteCombine(symbol, { fields }, {
		validateResult: false
	}) as any;

	// Default null/undefined values to 0
	const preMarketChangeVal = preMarketChange ?? 0;
	const regularMarketChangeVal = regularMarketChange ?? 0;
	const postMarketChangeVal = postMarketChange ?? 0;

	switch (marketState) {
		case "PRE":
			return {
				price: preMarketPrice,
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
				price: postMarketPrice,
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

// Export the Hono app
export default app;
