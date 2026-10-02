import http from "node:http";
import crypto from "node:crypto";
import QRCode from "qrcode";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || process.env.SERVER_PORT || 3110);
const PUBLIC_URL = (process.env.PUBLIC_URL || "").replace(/\/$/, "");
const sessions = new Map();

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  res.setHeader("access-control-allow-origin", "*");
  res.setHeader("access-control-allow-methods", "GET,POST,OPTIONS");
  res.setHeader("access-control-allow-headers", "content-type");
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  if (url.pathname === "/api/session" && req.method === "POST") {
    const session = crypto.randomBytes(12).toString("hex");
    const token = crypto.randomBytes(24).toString("hex");
    sessions.set(session, { token, peers: new Set(), created: Date.now() });
    const requestOrigin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host}`;
    const baseUrl = PUBLIC_URL || requestOrigin;
    const wsUrl = baseUrl.replace(/^https:/, "wss:").replace(/^http:/, "ws:");
    const payload = { v: 1, host: new URL(baseUrl).hostname, port: PORT, baseUrl, signalingUrl: `${wsUrl}/signal`, session, token };
    const qr = await QRCode.toDataURL(JSON.stringify(payload), { width: 360, margin: 2 });
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ...payload, qr }));
    return;
  }

  if (url.pathname === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ server, path: "/signal" });

wss.on("connection", ws => {
  let sessionId;
  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === "join") {
      const s = sessions.get(msg.session);
      if (!s || msg.token !== s.token || !["sender", "receiver"].includes(msg.role)) {
        ws.send(JSON.stringify({ type: "error", message: "Invalid or expired pairing session" }));
        ws.close();
        return;
      }
      sessionId = msg.session;
      s.peers.add(ws);
      ws.send(JSON.stringify({ type: "joined", role: msg.role }));
      return;
    }

    const s = sessionId && sessions.get(sessionId);
    if (!s) return;
    for (const peer of s.peers) {
      if (peer !== ws && peer.readyState === 1) peer.send(raw.toString());
    }
  });

  ws.on("close", () => {
    const s = sessionId && sessions.get(sessionId);
    if (s) {
      s.peers.delete(ws);
      if (s.peers.size === 0) sessions.delete(sessionId);
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Neuro Screen Share signaling server listening on ${PORT}`);
});
