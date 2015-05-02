#!/usr/bin/env bash
# NOTE: this script is only required for Macs. Apple removed sshpass so it needs to be added back to use
# Ansible to use ssh passwords.

cd ~/Downloads

curl -O -L http://downloads.sourceforge.net/project/sshpass/sshpass/1.05/sshpass-1.05.tar.gz

tar xvzf sshpass-1.05.tar.gz

cd sshpass-1.05

./configure

make

sudo make install
