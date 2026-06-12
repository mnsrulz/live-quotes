import { Hono } from "hono";
import { fromHono } from "chanfana";

export const tasksRouter = fromHono(new Hono());

tasksRouter.get("/", (c) => {
  return c.json({ message: "Hello, World!" });
});