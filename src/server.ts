import express, { Request, Response } from "express";
import { createClient } from "redis";

// Create a new Express application
const app = express();
const port = 3000;

// Middleware to parse JSON
app.use(express.json());

// Create a Redis client
const client = createClient({
    url: "redis://localhost:6379",
});

// Handle Redis connection errors
client.on("error", (err) => {
    console.error("Redis error:", err);
});

// Connect to Redis
client.connect().catch(console.error);

// Route to get all keys (indices)
app.get("/keys", async (req: Request, res: Response) => {
    try {
        const keys = await client.keys("*");
        res.send(keys);
    } catch (error: any) {
        res.status(500).send(error.toString());
    }
});

// Route to get a JSON value by key using POST
app.post("/keys", async (req: Request, res: Response) => {
    const key = req.body.key;
    if (!key) {
        return res.status(400).send("Key is required");
    }
    try {
        const value = await client.get(key);
        if (!value) {
            return res.status(404).send("Key not found");
        }
        // Try to parse JSON, if it fails, return the string
        try {
            const jsonValue = JSON.parse(value);
            res.send(jsonValue);
        } catch (e) {
            res.send(value);
        }
    } catch (error: any) {
        res.status(500).send(error.toString());
    }
});

// Start the server
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
