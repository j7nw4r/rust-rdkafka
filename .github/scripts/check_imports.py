"""Checks the imports of a Windows executable or DLL.

Usage: check_imports.py FILE [ITEM ...]. An ITEM is a DLL that must be
imported (bcrypt.dll), a function that must be imported
(secur32.dll:InitializeSecurityContextW), or a function that must not be
imported (!secur32.dll:InitializeSecurityContextA). No OpenSSL DLL may be
imported.
"""

import sys

import pefile

pe = pefile.PE(sys.argv[1])
imports = {}
for entry in pe.DIRECTORY_ENTRY_IMPORT:
    names = {imp.name.decode() for imp in entry.imports if imp.name}
    imports.setdefault(entry.dll.decode().lower(), set()).update(names)
print("imports:", " ".join(sorted(imports)))

errors = [f"OpenSSL import {dll}" for dll in imports
          if dll.startswith(("libssl", "libcrypto", "ssleay", "libeay"))]
for item in sys.argv[2:]:
    forbidden = item.startswith("!")
    dll, _, function = item.lstrip("!").lower().partition(":")
    if function:
        function = item.lstrip("!").partition(":")[2]
        present = function in imports.get(dll, set())
    else:
        present = dll in imports
    if present == forbidden:
        errors.append(f"{'unexpected' if forbidden else 'missing'} import {item.lstrip('!')}")
if errors:
    sys.exit("FAIL: " + "; ".join(errors))
print("PASS: imports as expected, no OpenSSL import")
