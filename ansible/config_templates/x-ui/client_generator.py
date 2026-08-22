#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import uuid
import random
import string
import sys
from datetime import datetime, timedelta

def random_email():
    name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=7))
    return f"{name}@xcplane"

def random_subid():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))

def generate_expiry(days: int) -> int:
    """
    Returns expiry time from input days for the API call
    """
    return int( -days * 3600 * 24 * 1000)

def generate_totalGB(traffic: int) -> int:
    """
    Generates totalGB value from input GB for the API call
    """
    return traffic * 1024 * 1024 * 1024

################################################################################

def main():
    if len(sys.argv) != 6:
        print("Usage: python generate_body.py <number_of_clients> <GB> <days> <comment> <inbound_id>")
        sys.exit(1)
        
    try:
        client_count, totalGB, expiry_time = [int(x) for x in sys.argv[1:4]]
    except ValueError:
        print("Client count, total GB and the expiry day must be integers.")
        sys.exit(1)
        
    try:
        client_comment = str(sys.argv[4])
    except ValueError:
        print("Client comment is malformed.")
        sys.exit(1)

    try:
        inbound_id = int(sys.argv[5])
    except ValueError:
        print("The inbound Id must be an integer.")
        sys.exit(1)
        
    clients = []

    for _ in range(client_count):
        item = {
            "client": {
                "id": str(uuid.uuid4()),
                # Not every inbound is compatible with xtls-rprx-vision
                "flow": "",
                "email": random_email(),
                "limitIp": 0,
                "totalGB": generate_totalGB(totalGB),
                "expiryTime": generate_expiry(expiry_time),
                "enable": True,
                "subId": random_subid(),
                "comment": client_comment,
                "reset": 0
            },
            "inboundIds": [inbound_id]
        }
        clients.append(item)

    json.dump(clients, sys.stdout, indent=2)

################################################################################

if __name__ == "__main__":
    main()
