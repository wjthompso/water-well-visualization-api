"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const cors_1 = __importDefault(require("cors"));
const express_1 = __importDefault(require("express"));
const redis_1 = require("redis");
// Create a new Express application
const app = (0, express_1.default)();
const port = 3000;
// Middleware to enable CORS
app.use((0, cors_1.default)());
// Middleware to parse JSON
app.use(express_1.default.json());
// Create a Redis client
const client = (0, redis_1.createClient)({
    url: "redis://localhost:6379",
});
// Handle Redis connection errors
client.on("error", (err) => {
    console.error("Redis error:", err);
});
// Connect to Redis
client.connect().catch(console.error);
// Function to parse the key into a Chunk
const parseKeyToChunk = (key) => {
    const match = key.match(/location:\(([^,]+), ([^,]+)\)-\(([^,]+), ([^,]+)\)/);
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
// Route to get all keys (indices) and return in Chunk[] format
app.get("/keys", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const keys = yield client.keys("*");
        const chunks = keys
            .map(parseKeyToChunk)
            .filter((chunk) => chunk !== null);
        res.send(chunks);
    }
    catch (error) {
        res.status(500).send(error.toString());
    }
}));
// Route to get a JSON value by key using POST
app.post("/keys", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const key = req.body.key;
    if (!key) {
        return res.status(400).send("Key is required");
    }
    try {
        const value = yield client.get(key);
        if (!value) {
            return res.status(404).send("Key not found");
        }
        // Try to parse JSON, if it fails, return the string
        try {
            const jsonValue = JSON.parse(value);
            res.send(jsonValue);
        }
        catch (e) {
            res.send(value);
        }
    }
    catch (error) {
        res.status(500).send(error.toString());
    }
}));
// Start the server
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
