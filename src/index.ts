import { ApiException, fromHono } from "chanfana";
import { Hono } from "hono";
import { tasksRouter } from "./endpoints/tasks/router";
import { ContentfulStatusCode } from "hono/utils/http-status";
import { streamSSE } from 'hono/streaming';
import YahooFinance from "yahoo-finance2";

// Start a Hono app
const app = new Hono<{ Bindings: Env }>();

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

// Setup OpenAPI registry
const openapi = fromHono(app, {
	docs_url: "/",
	schema: {
		info: {
			title: "My Awesome API",
			version: "2.0.0",
			description: "This is the documentation for my awesome API.",
		},
	},
});

// Register Tasks Sub router
openapi.route("/tasks", tasksRouter);

const yf = new YahooFinance({
	suppressNotices: ["yahooSurvey"], // optional
});

app.get("/events", (c) => {
	const normalizedSymbol = c.req.query("s") || 'AAPL';
	return streamSSE(c, async (stream) => {
		while (!stream.aborted) {
			const priceData = await fetchPrice(normalizedSymbol);
			await stream.writeSSE({
				data: JSON.stringify({
					t: Date.now(),
					...priceData
				}),
			})
			await stream.sleep(300)
		}
	})
});

async function fetchPrice(symbol: string) {
	const {
		marketState,
		regularMarketPrice,
		regularMarketChange,
		regularMarketChangePercent,
		postMarketPrice,
		postMarketChange,
		postMarketChangePercent,
		preMarketPrice,
		preMarketChange,
		preMarketChangePercent,
	} = await yf.quoteCombine(symbol, {}, {
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
				changePercent: preMarketChangePercent,
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
				change: regularMarketChangeVal + preMarketChangeVal,
				changePercent: regularMarketChangePercent + preMarketChangePercent,
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
				change: preMarketChangeVal + regularMarketChangeVal + postMarketChangeVal,
				changePercent: regularMarketChangePercent + preMarketChangePercent + postMarketChangePercent,
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
