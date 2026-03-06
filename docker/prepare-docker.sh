#!/usr/bin/bash
#
apt update
apt upgrade -y

export DEBIAN_FRONTEND=noninteractive
apt install -y \
    curl \
    git \
    locales \
    openssl \
    python3 \
    sudo \
    tzdata \
    vim
    wget \

echo "Europe/Oslo" > /etc/timezone
dpkg-reconfigure -f noninteractive tzdata

export LANGUAGE=en_EN.UTF-8
export LANG=en_EN.UTF-8
export LC_ALL=en_EN.UTF-8

locale-gen en_EN.UTF-8
dpkg-reconfigure locales

useradd -ms /bin/bash docker
echo "docker ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
chown -R docker:docker /home/docker

git config --global user.email "roehr@simula.no"
git config --global user.name "Thomas Roehr"
