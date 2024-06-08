from scapy.all import ARP, Ether, srp

def get_mac(ip_address):
    # Create an ARP request packet to get the MAC address corresponding to a given IP address
    arp_request = ARP(pdst=ip_address)
    broadcast = Ether(dst="ff:ff:ff:ff:ff:ff")
    arp_request_broadcast = broadcast/arp_request

    answered_list = srp(arp_request_broadcast, timeout=1, verbose=False)[0]

    # Return the MAC address from the ARP response (if we received a response)
    return answered_list[0][1].hwsrc if answered_list else None

ip = "10.2.1.82"  # Replace with the target IP address
mac_address = get_mac(ip)
if mac_address:
    print(f"MAC address of {ip} is {mac_address}")
else:
    print(f"Could not determine MAC address for {ip}")
