#!/bin/bash

# Установка зависимостей
sudo apt update

sudo apt install python3-dev libkrb5-dev
sudo apt-get install python3-venv
python3 -m venv venv
source venv/bin/activate
pip3 install -i http://172.16.254.115:8081/repository/pypi/simple/ --trusted-host 172.16.254.115 --upgrade pip==20.3.4
pip3 install -i http://172.16.254.115:8081/repository/pypi/simple/ --trusted-host 172.16.254.115 -r requirements.txt

# Redis
sudo apt-get install redis-server
sudo systemctl enable redis
sudo systemctl start redis

echo "Please make follow steps to setting Redis:"
echo "1. set 'bind 0.0.0.0 ::1' and 'bind 127.0.0.1 172.16.254.103' to the /etc/redis/redis.conf file."
echo "2. set 'notify-keyspace-events KA' to the /etc/redis/redis.conf file."
echo "3. restart redis-server by 'sudo systemctl restart redis'."
