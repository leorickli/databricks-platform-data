"""
Databricks Secrets Manager — Dev & Prod (OAuth, no PATs)
=========================================================
Manages secret scopes and secrets across two Databricks workspaces using
OAuth U2M authentication. Tokens auto-refresh — no expiring PATs to manage.

ONE-TIME SETUP
--------------
1. Install tools:
     pip install databricks-sdk
     pip install databricks-cli         # or: brew install databricks (macOS)

2. Log in to each workspace (opens browser for SSO, stores OAuth tokens
   in ~/.databrickscfg — tokens are auto-refreshed by the SDK):

     databricks auth login --host https://<dev-workspace>.azuredatabricks.net  --profile DEV
     databricks auth login --host https://<prod-workspace>.azuredatabricks.net --profile PROD

3. Verify:
     databricks auth profiles
     # should list DEV and PROD as VALID

That's it. Run this script anytime — no tokens, no env vars, no secrets to rotate.
"""

import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclPermission


# ──────────────────────────────────────────────
# Configuration — map env name to ~/.databrickscfg profile
# ──────────────────────────────────────────────
# Each value is a ~/.databrickscfg profile created via:
#   databricks auth login --host https://<workspace>.cloud.databricks.com --profile <name>

PROFILES = {
    "dev":  "dpx-dev",
    "stg":  "dpx-stg",
    "prod": "dpx-prod",
}


def get_client(env: str) -> WorkspaceClient:
    """Return a WorkspaceClient authenticated via the configured OAuth profile."""
    env = env.lower()
    if env not in PROFILES:
        raise ValueError(f"env must be one of {list(PROFILES)}, got '{env}'")
    return WorkspaceClient(profile=PROFILES[env])


def get_all_clients() -> dict[str, WorkspaceClient]:
    """Return clients for every env whose profile is configured in ~/.databrickscfg.

    Profiles listed in PROFILES but not yet set up (no `databricks auth login`)
    are skipped with a warning instead of failing the whole run.
    """
    clients = {}
    for env in PROFILES:
        try:
            clients[env] = get_client(env)
        except ValueError as e:
            print(f"[{env.upper()}] [skip] profile not configured — {e}")
    return clients


# ──────────────────────────────────────────────
# Scopes
# ──────────────────────────────────────────────

def list_scopes(w: WorkspaceClient, env: str) -> None:
    scopes = list(w.secrets.list_scopes())
    print(f"\n[{env.upper()}] Secret Scopes ({len(scopes)} found)")
    print(f"  {'Scope Name':<40} Backend")
    print("  " + "-" * 55)
    for s in scopes or []:
        backend = s.backend_type.value if s.backend_type else "DATABRICKS"
        print(f"  {s.name:<40} {backend}")
    if not scopes:
        print("  (none)")


def create_scope(w: WorkspaceClient, env: str, scope: str) -> None:
    existing = {s.name for s in w.secrets.list_scopes()}
    if scope in existing:
        print(f"[{env.upper()}] [skip] Scope '{scope}' already exists.")
        return
    w.secrets.create_scope(scope)
    print(f"[{env.upper()}] [ok]   Scope '{scope}' created.")


def delete_scope(w: WorkspaceClient, env: str, scope: str) -> None:
    w.secrets.delete_scope(scope)
    print(f"[{env.upper()}] [ok]   Scope '{scope}' deleted.")


# ──────────────────────────────────────────────
# Secrets
# ──────────────────────────────────────────────

def list_secrets(w: WorkspaceClient, env: str, scope: str) -> None:
    secrets = list(w.secrets.list_secrets(scope))
    print(f"\n[{env.upper()}] Secrets in '{scope}' ({len(secrets)} found)")
    print(f"  {'Key':<40} Last Updated")
    print("  " + "-" * 65)
    for s in secrets or []:
        ts = s.last_updated_timestamp
        updated = (
            "unknown" if ts is None
            else datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
        )
        print(f"  {s.key:<40} {updated}")
    if not secrets:
        print("  (none)")


def put_secret(w: WorkspaceClient, env: str, scope: str, key: str, value: str) -> None:
    w.secrets.put_secret(scope, key, string_value=value)
    print(f"[{env.upper()}] [ok]   '{scope}/{key}' set.")


def put_secrets_bulk(
    w: WorkspaceClient, env: str, scope: str, secrets: dict[str, str]
) -> None:
    for key, value in secrets.items():
        put_secret(w, env, scope, key, value)


def delete_secret(w: WorkspaceClient, env: str, scope: str, key: str) -> None:
    w.secrets.delete_secret(scope, key)
    print(f"[{env.upper()}] [ok]   '{scope}/{key}' deleted.")


def copy_scope(
    clients: dict, src_env: str, dst_env: str, scope: str, *, overwrite: bool = False
) -> None:
    """Copy every secret in `scope` from one workspace to another.

    Reads the actual values from the source via get_secret (requires READ on the
    source scope), creates the scope in the destination if missing, then writes
    each key. Values never need to be known/pasted by hand.

    Set overwrite=False to skip keys that already exist in the destination.
    """
    src, dst = clients[src_env], clients[dst_env]

    create_scope(dst, dst_env, scope)
    existing = {} if overwrite else {s.key for s in dst.secrets.list_secrets(scope)}

    keys = [s.key for s in src.secrets.list_secrets(scope)]
    print(f"[{src_env.upper()}→{dst_env.upper()}] Copying {len(keys)} key(s) from '{scope}'")
    for key in keys:
        if key in existing:
            print(f"[{dst_env.upper()}] [skip] '{scope}/{key}' already exists.")
            continue
        # get_secret().value and put_secret(bytes_value=...) both use base64, so
        # the value passes straight through — never decoded or printed in clear.
        value_b64 = src.secrets.get_secret(scope, key).value
        dst.secrets.put_secret(scope, key, bytes_value=value_b64)
        print(f"[{dst_env.upper()}] [ok]   '{scope}/{key}' copied.")


# ──────────────────────────────────────────────
# ACLs
# ──────────────────────────────────────────────

def grant_scope_access(
    w: WorkspaceClient, env: str, scope: str, principal: str, permission: str = "READ"
) -> None:
    perm = AclPermission[permission.upper()]
    w.secrets.put_acl(scope, principal, perm)
    print(f"[{env.upper()}] [ok]   Granted {permission} on '{scope}' to '{principal}'.")


# ──────────────────────────────────────────────
# Multi-environment helpers
# ──────────────────────────────────────────────

def deploy_scope_to_all(clients: dict, scope: str) -> None:
    """Create a scope in every environment."""
    for env, w in clients.items():
        create_scope(w, env, scope)


def deploy_secrets_per_env(
    clients: dict, scope: str, secrets_by_env: dict[str, dict[str, str]]
) -> None:
    """
    Deploy different values per env.

    secrets_by_env = {
        "dev":  {"key": "dev-value", ...},
        "prod": {"key": "prod-value", ...},
    }
    """
    for env, w in clients.items():
        env_secrets = secrets_by_env.get(env, {})
        if env_secrets:
            put_secrets_bulk(w, env, scope, env_secrets)
        else:
            print(f"[{env.upper()}] [skip] No secrets defined.")


def deploy_shared_secrets(
    clients: dict, scope: str, shared: dict[str, str]
) -> None:
    """Push the same secrets to every environment."""
    for env, w in clients.items():
        put_secrets_bulk(w, env, scope, shared)


if __name__ == "__main__":

    clients = get_all_clients()

    # Confirm connection to each workspace
    # for env, w in clients.items():
    #     print(f"[{env.upper():<4}] connected to: {w.config.host}")

    # ── List scopes in both envs ──────────────
    # for env, w in clients.items():
    #     list_scopes(w, env)
    
    # ── DEPLOYMENTS ──

    # ── Create a scope in both envs ───────────
    # deploy_scope_to_all(clients, "acme_smartnode_api_creds")

    # ── Deploy env-specific secrets ───────────
    # deploy_secrets_per_env(clients, "my-app-secrets", {
    #     "dev": {
    #         "db-host":     "dev-db.internal",
    #         "db-password": "dev-password",
    #         "api-key":     "dev-api-key-123",
    #         "log-level":   "DEBUG",
    #     },
    #     "prod": {
    #         "db-host":     "prod-db.internal",
    #         "db-password": "prod-password",
    #         "api-key":     "prod-api-key-456",
    #         "log-level":   "WARNING",
    #     },
    # })

    # ── Deploy shared secrets (same in both) ──
    # deploy_shared_secrets(clients, "acme_smartnode_api_creds", {
    #     "base_url": "https://smartnode.eu/login/api/v1",
    #     "api_key": "getQkU2OEZDMkQ1NjJEREJGNDk2Mjg2QUY2dataplatform",
    # })

    # ── Copy a whole scope between envs ───────
    # Reads each value from the source via get_secret and writes it to the
    # destination — values never need to be known/pasted. Skips keys that
    # already exist in the destination (pass overwrite=True to force).
    # NOTE: copies values only, not ACLs — grant READ to the run-as principal after.
    copy_scope(clients, "prod", "stg", "acme_ampcore_api_creds")
    copy_scope(clients, "prod", "stg", "acme_smartnode_api_creds")

    # ── Verify ─────────────────────────────────
    list_secrets(clients["stg"], "stg", "acme_ampcore_api_creds")
    list_secrets(clients["stg"], "stg", "acme_smartnode_api_creds")

    # ── Rotate a single secret in prod only ────
    # put_secret(clients["prod"], "prod", "my-app-secrets", "api-key", "new-value")

    # ── Delete a secret from both envs ─────────
    # for env, w in clients.items():
    #     delete_secret(w, env, "my-app-secrets", "old-key")

    # ── Delete a scope from both envs ──────────
    # for env, w in clients.items():
    #     delete_scope(w, env, "my-app-secrets")

    # ── Grant ACL access in both envs ──────────
    # for env, w in clients.items():
    #     grant_scope_access(w, env, "my-app-secrets", "user@example.com", "READ")
