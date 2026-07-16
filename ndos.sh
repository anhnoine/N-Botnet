#!/usr/bin/env bash
clear
apt update
apt install git -y
clear
sleep 5
bash <(curl -s https://raw.githubusercontent.com/anhnoine/n-manios/refs/heads/main/nPlugins/nplugin_install.sh)
rm nSocks.c
rm n-args.c
clear
wget https://raw.githubusercontent.com/anhnoine/n-manios/refs/heads/main/nPlugins/plugins/nSocks.c
wget https://raw.githubusercontent.com/anhnoine/n-manios/refs/heads/main/nPlugins/plugins/n-args.c
nplugin install -f nSocks.c
nplugin install -f n-args.c
rm nSocks.c
rm n-args.c
clear
