"""Checks that a Windows executable imports the native TLS and crypto DLLs
and no OpenSSL DLL. Usage: check_imports.py EXE [REQUIRED_DLL ...]."""

import sys

import pefile

pe = pefile.PE(sys.argv[1])
dlls = sorted({entry.dll.decode().lower() for entry in pe.DIRECTORY_ENTRY_IMPORT})
print("imports:", " ".join(dlls))
openssl = [dll for dll in dlls if dll.startswith(("libssl", "libcrypto", "ssleay", "libeay"))]
missing = [dll.lower() for dll in sys.argv[2:] if dll.lower() not in dlls]
if openssl or missing:
    sys.exit(f"FAIL: OpenSSL imports {openssl}, missing imports {missing}")
print("PASS: no OpenSSL import")
