#! /bin/bash
# Changing resolv.conf back to the DoH at startup
/bin/bash -c 'echo "nameserver 127.0.2.1" > /etc/resolv.conf' && echo "resolv.conf changed."
