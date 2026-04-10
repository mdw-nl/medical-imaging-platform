# Security Deployment Notes

## Network: Restricting DICOM Port Access

Port 104 (DICOM C-STORE listener) is exposed to the host network so treatment planning
systems can send DICOM data. Restrict access to known source IPs using iptables:

```bash
# Replace with the actual IPs of your treatment planning systems
ALLOWED_IPS="10.0.1.50 10.0.1.51"

# Drop all traffic to port 104 except from allowed IPs
for ip in $ALLOWED_IPS; do
    sudo iptables -A INPUT -p tcp --dport 104 -s "$ip" -j ACCEPT
done
sudo iptables -A INPUT -p tcp --dport 104 -j DROP

# Persist rules across reboots (Debian/Ubuntu)
sudo apt-get install -y iptables-persistent
sudo netfilter-persistent save
```

## Network: Services Bound to Localhost

The following services are bound to `127.0.0.1` and are **not** reachable from other
machines on the network:

- PostgreSQL (5432)
- XNAT DICOM SCP (8104)

The imaging-hub API port is not exposed externally. Internal
services reach it via the Docker bridge network (`http://imaging-hub:9000`).

## Network: Services Exposed to the Network

The following services listen on all interfaces (`0.0.0.0`) and are reachable from
other machines on the network:

- XNAT web UI (8080)
- Grafana (3000)

Both UIs require authentication, consider restricting access
with firewall rules.

## Credentials

All passwords must be set in `deploy/.env` before starting the stack. The compose
file will refuse to start if any required secret is missing. See `.env.example` for the
full list.

Generate strong random passwords:

```bash
openssl rand -base64 32
```

## XNAT Anonymization

XNAT SCP receivers have `anonymizationEnabled: false` because the imaging-hub
anonymizes all DICOM data before forwarding to XNAT. If the imaging-hub is ever
bypassed (e.g., direct DICOM send to XNAT port 8104), XNAT will store un-anonymized
PHI. To prevent this, keep XNAT's DICOM port bound to localhost (as configured)
and only allow the pacs-archiver container to send to it via the Docker network.
