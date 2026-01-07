import argparse
import configparser
import os
import shutil
import subprocess
import sys
from pathlib import Path

DEFAULT_WORKSPACE_DIR = None
ENV_VARS = (
    "DATABRICKS_HOST",
    "DATABRICKS_ACCOUNT_ID",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
)

def run(cmd):
    print(f"\n$ {' '.join(cmd)}")
    subprocess.check_call(cmd)

def run_capture(cmd):
    print(f"\n$ {' '.join(cmd)}")
    return subprocess.check_output(cmd, text=True).strip()

def ensure_databricks_cli():
    if shutil.which("databricks") is None:
        raise RuntimeError("Databricks CLI not found. Install it and retry.")

def load_env_file(path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export "):].strip()
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value

def configure_oauth_env(use_cli_profile):
    if use_cli_profile:
        return
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if not client_id or not client_secret:
        return
    os.environ.setdefault("DATABRICKS_AUTH_TYPE", "oauth-m2m")

def resolve_profile(cli_profile):
    if cli_profile:
        return cli_profile
    return os.environ.get("DATABRICKS_PROFILE") or os.environ.get("DATABRICKS_CONFIG_PROFILE")

def get_databricks_workspace_user():
    return (
        os.environ.get("DATABRICKS_WORKSPACE_USER")
        or os.environ.get("AZUREAD_ADMIN_LOGIN")
        or os.environ.get("DATABRICKS_USER")
        or os.environ.get("DATABRICKS_USERNAME")
    )

def get_default_workspace_dir():
    user = get_databricks_workspace_user()
    if not user:
        raise RuntimeError(
            "Set DATABRICKS_WORKSPACE_USER (or AZUREAD_ADMIN_LOGIN) or pass --workspace-dir."
        )
    return f"/Users/{user}/spotify_dab"

def normalize_host(value):
    if not value:
        return None
    return value.strip().rstrip("/")

def find_profile_for_host(host):
    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        return None, False
    host_norm = normalize_host(host)
    if not host_norm:
        return None, False
    parser = configparser.RawConfigParser()
    parser.read(cfg_path)
    matches = []
    for section in parser.sections():
        cfg_host = parser.get(section, "host", fallback=None)
        if cfg_host and normalize_host(cfg_host) == host_norm:
            matches.append(section)
    default_host = parser.defaults().get("host")
    default_match = default_host and normalize_host(default_host) == host_norm
    if matches:
        if len(matches) == 1:
            return matches[0], True
        print("Multiple Databricks CLI profiles match the host. Use --profile to select one.")
        return None, False
    if default_match:
        return None, True
    return None, False

def databricks_cmd(profile, *args):
    cmd = ["databricks"]
    if profile:
        cmd.extend(["--profile", profile])
    cmd.extend(args)
    return cmd

def detect_host(repo_root):
    env_host = os.environ.get("DATABRICKS_HOST")
    if env_host:
        return env_host
    tf_dir = repo_root / "terraform" / "08_databricks"
    if tf_dir.exists():
        try:
            return run_capture(
                ["terraform", f"-chdir={tf_dir}", "output", "-raw", "databricks_workspace_url"]
            )
        except subprocess.CalledProcessError:
            return None
    return None

def main():
    parser = argparse.ArgumentParser(
        description="Push local Databricks workspace content to the Databricks workspace."
    )
    parser.add_argument(
        "--workspace-dir",
        default=DEFAULT_WORKSPACE_DIR,
        help="Destination workspace directory (default: /Users/<user>/spotify_dab).",
    )
    parser.add_argument(
        "--profile",
        help="Databricks CLI profile to use (or set DATABRICKS_PROFILE/DATABRICKS_CONFIG_PROFILE).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    load_env_file(repo_root / ".env")
    local_dir = repo_root / "spotify_dab"
    if not local_dir.exists():
        raise FileNotFoundError(f"Missing local workspace dir: {local_dir}")
    workspace_dir = args.workspace_dir or get_default_workspace_dir()

    ensure_databricks_cli()
    host = detect_host(repo_root)
    if not host:
        raise RuntimeError("DATABRICKS_HOST not set and workspace URL not found in Terraform outputs.")
    os.environ.setdefault("DATABRICKS_HOST", host)
    profile = resolve_profile(args.profile)
    use_cli_profile = bool(profile)
    if not profile:
        auto_profile, auto_use_cli = find_profile_for_host(host)
        if auto_profile:
            profile = auto_profile
            use_cli_profile = True
        elif auto_use_cli:
            use_cli_profile = True
    configure_oauth_env(use_cli_profile)

    run(databricks_cmd(profile, "workspace", "mkdirs", workspace_dir))
    run(databricks_cmd(profile, "workspace", "import-dir", "--overwrite", str(local_dir), workspace_dir))
    print(f"Imported {local_dir} -> {workspace_dir}")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}")
        sys.exit(1)
