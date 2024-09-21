import cors from "cors";
import express, { Request, Response, NextFunction } from "express";
import { createClient } from "redis";
import dotenv from "dotenv";
import path from "path";  // Import path module

// Load environment variables from a .env file
dotenv.config();

// Create a new Express application
const app = express();
const port = process.env.PORT || 3000;

// Middleware to enable CORS
app.use(cors());

// Middleware to parse JSON
app.use(express.json());

// Create a Redis client
const client = createClient({
    url: process.env.REDIS_URL || "redis://localhost:6379",
});

// Handle Redis connection errors
client.on("error", (err) => {
    console.error("Redis error:", err);
});

// Connect to Redis
client.connect().catch(console.error);

// Interface for Chunk
interface Chunk {
    topLeft: {
        lat: number;
        lon: number;
    };
    bottomRight: {
        lat: number;
        lon: number;
    };
}

// Function to parse the key into a Chunk
const parseKeyToChunk = (key: string): Chunk | null => {
    const match = key.match(
        /location:\(([^,]+), ([^,]+)\)-\(([^,]+), ([^,]+)\)/
    );
    if (!match) {
        return null;
    }
    return {
        topLeft: {
            lat: parseFloat(match[1]),
            lon: parseFloat(match[2]),
        },
        bottomRight: {
            lat: parseFloat(match[3]),
            lon: parseFloat(match[4]),
        },
    };
};

// Used to verify server
app.get("/.well-known/pki-validation/46B38693C6AFEAE0600108325FE8A834.txt", (req: Request, res: Response) => {
    const filePath = path.join(__dirname, "../../.well-known/pki-validation/", "46B38693C6AFEAE0600108325FE8A834.txt");
    res.sendFile(filePath);
});


// Route to get all keys (indices) and return in Chunk[] format
app.get("/keys", async (req: Request, res: Response, next: NextFunction) => {
    try {
        const keys = await client.keys("*");
        const chunks = keys
            .map(parseKeyToChunk)
            .filter((chunk): chunk is Chunk => chunk !== null);
        res.send(chunks);
    } catch (error) {
        next(error);
    }
});

// Route to get a JSON value by key using POST
app.post("/keys", async (req: Request, res: Response, next: NextFunction) => {
    const key = req.body.key;
    if (!key) {
        return res.status(400).send("Key is required");
    }
    try {
        const value = await client.get(key);
        if (!value) {
            return res.status(404).send("Key not found");
        }
        try {
            const jsonValue = JSON.parse(value);
            res.send(jsonValue);
        } catch (e) {
            res.send(value);
        }
    } catch (error) {
        next(error);
    }
});

// Error handling middleware
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
    console.error(err);
    res.status(500).json({ error: err.message });
});

// Graceful shutdown
const shutdown = () => {
    client.quit().then(() => {
        console.log("Redis client disconnected.");
        process.exit(0);
    }).catch((err) => {
        console.error("Error during Redis client shutdown:", err);
        process.exit(1);
    });
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Start the server
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
