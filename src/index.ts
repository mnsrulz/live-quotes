import { Hono } from "hono";
import { streamSSE } from 'hono/streaming';
import YahooFinance from "yahoo-finance2";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import isToday from "dayjs/plugin/isToday";
import timezone from "dayjs/plugin/timezone";
import "dayjs/locale/en";

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(isToday);

const MAX_REQUESTS_PER_INVOCATION = 50;	// in CF - 60 requests is what they allow per invocation. So capping at 50 to be safe

// Start a Hono app
const app = new Hono();

app.onError((err, c) => {
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

app.get('/price', async (c) => {
	const s = c.req.query("s") || '';
	const dt = c.req.query("dt") || '';
	const f = (c.req.query("f") || '0') == '1';
	const o = (c.req.query("o") || '1') == '1';	//keep original value

	const price = await getPriceAtDate(s, dt, f, o);

	return c.json({
		price
	})
})

app.get('/historical-prices', async (c) => {
	const s = c.req.query("s") || '';
	const n = parseInt(c.req.query("n") || '7');
	const i = (c.req.query("i") || 'd') as 'd' | 'h';
	
	const prices = await getLastNPrices(s, n, i);

	return c.json({
		prices
	})
})

app.get('/ohlc', async (c) => {
	const s = c.req.query("s") || '';
	const prices = await getLastNPrices(s, 365, 'd');
	return c.json(prices);
})

app.get("/live-quotes", (c) => {
	const normalizedSymbol = new Set(c.req.query("s")?.split(',').map(s => s.trim().toUpperCase()).filter(Boolean) || ['AAPL']);
	const interval = c.req.query("i") ? parseInt(c.req.query("i")!) : 1000;
	let requestCount = 0;
	return streamSSE(c, async (stream) => {
		let anySuccessfulFetch = false;
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
				anySuccessfulFetch = true;
			} catch (error) {
				console.error(`Error fetching price for ${symbol}:`, error);
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

const EXCEPTION_SYMBOLS = {
	'SPX': '^SPX',
	'VIX': '^VIX',
} as Record<string, string>


async function getPriceAtDate(symbol: string, dt: string, fallbackToPreviousDayWhenNoPriceFound: boolean, keepOriginalValue: boolean) {
	try {
		const start = dayjs(dt.substring(0, 10)).format('YYYY-MM-DD');
		const resp = await yf.chart(EXCEPTION_SYMBOLS[symbol.toUpperCase()] || symbol, {
			interval: '1d',
			period1: dayjs(start).add(-7, 'day').toDate(),
			period2: dayjs(start).toDate()
		})
		const priceToReturn = fallbackToPreviousDayWhenNoPriceFound ? resp.quotes.reverse().find(k => k.close != null)?.close : resp.quotes.at(-1)?.close;
		return keepOriginalValue ? priceToReturn : priceToReturn?.toFixed(2);
	} catch (error) {
		console.error(`Error fetching price for ${symbol} on ${dt}:`, error);
		return null;
	}
}

export const getLastNPrices = async (symbol: string, lastN: number, interval: 'd' | 'h') => {
    const t = Math.ceil(interval == 'h' ? Math.ceil((lastN * 1.2) / 40) : Math.ceil((lastN * 1.2) / 5));       //take extra couple days of data just to be sure we have enough data
    const start = dayjs().format('YYYY-MM-DD');
    const resp = await yf.chart(EXCEPTION_SYMBOLS[symbol.toUpperCase()] || symbol, {
        interval: interval == 'd' ? '1d' : '1h',
        period1: dayjs(start).add(-t, 'week').toDate(),
        period2: dayjs(start).toDate()
    })
    return resp.quotes.map(({ close, date, open, high, low, volume, adjclose }) => ({ close, date, open, high, low, volume, adjclose }))
		.filter(k => k.close != null)
		.map(({ close, date, adjclose, high, low, open, volume }) => ({ date: date.toISOString().slice(0, 10), close: Number(close), open, high, low, volume, adjclose })).slice(-lastN);
}

// Export the Hono app
export default app;
