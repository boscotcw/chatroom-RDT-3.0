# RDT 3.0 Chatroom

Multi-user chatroom implementing RDT 3.0, coded in C.

Programming assignment of HKUST course COMP4621 (Computer and Communication Networks). 

Supports one TCP server, multiple TCP clients, and UDP peer-to-peer messaging. 

## Overview

### Main features: 
- Server handles multiple clients with `poll()` and manages user information state and message broadcasting.
- Clients use threads to send commands and receive messages concurrently from the server and peers.
- Peer-to-peer direct messaging over UDP with a reliable data transfer protocol (RDT 3.0).

Other features include registration and login, user directory lookup, and offline message storage.



Please see [PA report_20956530.tex](https://github.com/boscotcw/chatroom-RDT-3.0/blob/main/PA%20report_20956530.tex) for more details.
