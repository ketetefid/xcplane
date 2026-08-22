#! /bin/bash
# systemD doesn't know bash directly
/bin/bash -c 'echo "nameserver 8.8.8.8" > /etc/resolv.conf'
/bin/bash -c 'echo "nameserver 1.1.1.1" >> /etc/resolv.conf'
