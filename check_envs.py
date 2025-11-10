"""Check required environment variables and module accessibility.

This script reports whether necessary environment variables are present and
attempts to import the main project modules to ensure the master script can
access them. It never prints secret values (only presence and safe metadata).
"""
import os
import importlib
import sys


REQUIRED_ENVS = [
    "AZURE_STORAGE_CONNECTION_STRING",
]

OPTIONAL_ENVS = [
    "AZURE_BLOB_CONTAINER",
    "DATASET_LIMIT",
    "BASE_PATH",
    "LOG_LEVEL",
    "PG_HOST",
    "PG_USER",
    "PG_PASSWORD",
    "PG_DATABASE",
]

MODULES_TO_IMPORT = [
    "utils",
    "extract_ccvi",
    "extract_covid",
    "extract_health",
    "extract_permits",
    "extract_taxi",
    "extract_tnp",
    "chicago_dag",
]


def masked(s: str) -> str:
    if not s:
        return ""
    # show only first 4 and last 4 chars
    if len(s) <= 10:
        return "*" * len(s)
    return f"{s[:4]}...{s[-4:]}"


def check_envs():
    ok = True
    print("Environment variable check:")
    for name in REQUIRED_ENVS:
        val = os.environ.get(name)
        if val:
            # special-case connection strings: don't print full value
            print(f"  [OK] {name} present (length={len(val)})")
        else:
            print(f"  [MISSING] {name}")
            ok = False

    print("\nOptional environment variables:")
    for name in OPTIONAL_ENVS:
        val = os.environ.get(name)
        if val:
            # safe to show small masked value
            print(f"  [set] {name} = {masked(val)}")
        else:
            print(f"  [unset] {name}")

    return ok


def check_imports():
    print("\nModule import check:")
    success = True
    for mod in MODULES_TO_IMPORT:
        try:
            importlib.import_module(mod)
            print(f"  [OK] imported {mod}")
        except Exception as e:
            print(f"  [ERROR] importing {mod}: {e}")
            success = False
    return success


def main():
    env_ok = check_envs()
    import_ok = check_imports()

    print("\nSummary:")
    if env_ok and import_ok:
        print("  All required env vars present and modules importable.")
        sys.exit(0)
    else:
        if not env_ok:
            print("  One or more required environment variables are missing.")
        if not import_ok:
            print("  One or more modules failed to import. See above for details.")
        sys.exit(2)


if __name__ == "__main__":
    main()
