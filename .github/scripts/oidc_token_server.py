"""Test-only OAuth token endpoint for the schannel CI job.

It makes a private CA, an HTTPS server certificate for localhost, a second
unrelated CA, and RS256/ES256 assertion keys. It serves POST /token over
HTTPS and the CA's CRL over plain HTTP, so that Schannel can check
revocation. With --no-cdp the server certificate names no CRL. Each token request is appended to requests.jsonl. A
client_credentials request must use the test client id and secret. A
jwt-bearer assertion must carry a valid signature from one of the assertion
keys. A valid request gets an unsigned access token with `sub` and `exp`.
"""

import argparse
import base64
import datetime as dt
import http.server
import json
import pathlib
import ssl
import threading
import time
import urllib.parse

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

CLIENT_ID = "tls-check-client"
CLIENT_SECRET = "tls-check-secret"
JWT_BEARER = "urn:ietf:params:oauth:grant-type:jwt-bearer"


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def b64url_decode(text):
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def pem_key(key):
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


def make_credentials(out, crl_url, cdp):
    now = dt.datetime.now(dt.timezone.utc)
    start, end = now - dt.timedelta(hours=1), now + dt.timedelta(days=2)

    def name(cn):
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])

    def ca_cert(cn, key):
        return (x509.CertificateBuilder().subject_name(name(cn)).issuer_name(name(cn))
                .public_key(key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(start).not_valid_after(end)
                .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                .add_extension(x509.KeyUsage(False, False, False, False, False, True, True,
                                             False, False), critical=True)
                .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False)
                .sign(key, hashes.SHA256()))

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca = ca_cert("tls-check test CA", ca_key)
    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    other = ca_cert("tls-check unrelated CA", other_key)

    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server = (x509.CertificateBuilder().subject_name(name("localhost")).issuer_name(ca.subject)
              .public_key(server_key.public_key()).serial_number(x509.random_serial_number())
              .not_valid_before(start).not_valid_after(end)
              .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
              .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
              .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), False)
              .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(
                  ca_key.public_key()), False))
    if cdp:
        server = server.add_extension(x509.CRLDistributionPoints([x509.DistributionPoint(
            [x509.UniformResourceIdentifier(crl_url)], None, None, None)]), False)
    server = server.sign(ca_key, hashes.SHA256())
    crl = (x509.CertificateRevocationListBuilder().issuer_name(ca.subject)
           .last_update(start).next_update(end).sign(ca_key, hashes.SHA256()))

    rs256 = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    es256 = ec.generate_private_key(ec.SECP256R1())
    pem = serialization.Encoding.PEM
    (out / "ca.pem").write_bytes(ca.public_bytes(pem))
    (out / "wrong-ca.pem").write_bytes(other.public_bytes(pem))
    (out / "server.pem").write_bytes(server.public_bytes(pem))
    (out / "server.key").write_bytes(pem_key(server_key))
    (out / "assertion-rs256.key").write_bytes(pem_key(rs256))
    (out / "assertion-es256.key").write_bytes(pem_key(es256))
    return crl.public_bytes(serialization.Encoding.DER), rs256.public_key(), es256.public_key()


def verify_assertion(assertion, rs256, es256):
    header, payload, signature = assertion.split(".")
    alg = json.loads(b64url_decode(header))["alg"]
    signed = f"{header}.{payload}".encode()
    sig = b64url_decode(signature)
    try:
        if alg == "RS256":
            rs256.verify(sig, signed, padding.PKCS1v15(), hashes.SHA256())
        elif alg == "ES256":
            r, s = int.from_bytes(sig[:32], "big"), int.from_bytes(sig[32:], "big")
            es256.verify(encode_dss_signature(r, s), signed, ec.ECDSA(hashes.SHA256()))
        else:
            return alg, False
    except InvalidSignature:
        return alg, False
    return alg, True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, type=pathlib.Path)
    parser.add_argument("--no-cdp", action="store_true")
    args = parser.parse_args()
    out = args.dir
    out.mkdir(parents=True, exist_ok=True)
    log = out / "requests.jsonl"

    state = {}

    class CrlHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/pkix-crl")
            self.send_header("Content-Length", str(len(state["crl"])))
            self.end_headers()
            self.wfile.write(state["crl"])

    class TokenHandler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode()
            form = urllib.parse.parse_qs(body)
            grant = form.get("grant_type", [""])[0]
            record = {"path": self.path, "grant_type": grant}
            if grant == "client_credentials":
                auth = self.headers.get("Authorization", "")
                expected = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
                record["ok"] = auth == f"Basic {expected}"
            elif grant == JWT_BEARER:
                alg, ok = verify_assertion(form["assertion"][0], state["rs256"], state["es256"])
                record.update(alg=alg, ok=ok)
            else:
                record["ok"] = False
            with log.open("a") as f:
                f.write(json.dumps(record) + "\n")
            if record["ok"]:
                now = int(time.time())
                token = ".".join([b64url(b'{"alg":"RS256","typ":"JWT"}'),
                                  b64url(json.dumps({"sub": "tls-check", "iat": now,
                                                     "exp": now + 3600}).encode()),
                                  b64url(b"unsigned")])
                reply = {"access_token": token, "token_type": "bearer", "expires_in": 3600}
                status = 200
            else:
                reply, status = {"error": "invalid_client"}, 401
            data = json.dumps(reply).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    crl_server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CrlHandler)
    crl_url = f"http://127.0.0.1:{crl_server.server_address[1]}/ca.crl"
    state["crl"], state["rs256"], state["es256"] = make_credentials(out, crl_url, not args.no_cdp)

    token_server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), TokenHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(out / "server.pem", out / "server.key")
    token_server.socket = context.wrap_socket(token_server.socket, server_side=True)
    threading.Thread(target=crl_server.serve_forever, daemon=True).start()
    url = f"https://localhost:{token_server.server_address[1]}/token"
    (out / "token-url").write_text(url)
    print(f"token endpoint {url}, CRL {crl_url}", flush=True)
    token_server.serve_forever()


if __name__ == "__main__":
    main()
