import { Hono } from "https://esm.sh/hono@4.12";
import { handle } from "https://esm.sh/hono@4.12/netlify";


const app = new Hono();
app.get("/api3/hello", (c) => {
    return c.json({
        "hello": "test"
    });
});

export const config = { path: "/api3/*" };
export default handle(app);