"""CLI entry point for Tusk"""

import subprocess
import sys
from pathlib import Path


def main():
    """Entry point for tusk command"""
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    command = sys.argv[1]

    if command == "studio":
        start_studio()
    elif command == "config":
        handle_config()
    elif command == "scheduler":
        start_scheduler()
    elif command == "worker":
        start_worker()
    elif command == "cluster":
        start_cluster_dev()
    elif command == "users":
        handle_users()
    elif command == "auth":
        handle_auth()
    elif command == "features":
        show_features()
    elif command == "version":
        from tusk import __version__
        print(f"Tusk v{__version__}")
    elif command == "help" or command == "--help":
        print_usage()
    else:
        print(f"Unknown command: {command}")
        print_usage()
        sys.exit(1)


def show_features():
    """Show status of optional features"""
    from tusk.core.deps import print_feature_status
    print_feature_status()


def print_usage():
    """Print usage information"""
    print("""
Tusk - Modern Data Platform

Usage:
    tusk studio [options]     Start the web studio        [requires: studio]
    tusk config [options]     Manage configuration
    tusk scheduler [options]  Start the cluster scheduler [requires: cluster]
    tusk worker [options]     Start a cluster worker      [requires: cluster]
    tusk cluster [options]    Start local cluster (dev)   [requires: cluster]
    tusk users [command]      Manage users                [requires: studio]
    tusk auth [command]       Manage authentication       [requires: studio]
    tusk features             Show installed features
    tusk version              Show version
    tusk help                 Show this help

Studio Options:
    --host HOST               Host to bind to (default: 127.0.0.1)
    --port, -p PORT           Port to bind to (default: 8000)
    --pg-bin-path PATH        Path to PostgreSQL binaries (pg_dump, psql)

Scheduler Options:
    --host HOST               Host to bind to (default: 0.0.0.0)
    --port, -p PORT           Port to bind to (default: 8814)

Worker Options:
    --scheduler HOST:PORT     Scheduler address (default: localhost:8814)
    --host HOST               Host to bind to (default: 0.0.0.0)
    --port, -p PORT           Port to bind to (default: 8815)

Cluster Options (dev mode):
    --workers, -w N           Number of workers (default: 3)

Config Commands:
    tusk config show                    Show current configuration
    tusk config set KEY VALUE           Set a configuration value
    tusk config set pg_bin_path PATH    Set PostgreSQL binaries path

User Commands:
    tusk users list                     List all users
    tusk users create USERNAME          Create a new user
    tusk users delete USERNAME          Delete a user
    tusk users reset-password USERNAME  Reset user password

Auth Commands:
    tusk auth init                      Initialize auth (create admin user)
    tusk auth enable                    Enable multi-user mode
    tusk auth disable                   Disable auth (single mode)

Examples:
    tusk studio
    tusk studio --port 3000
    tusk scheduler --port 8814
    tusk worker --scheduler localhost:8814 --port 8815
    tusk cluster --workers 3
    tusk config show
    tusk auth enable
    tusk users create admin --admin
""")


def start_studio():
    """Start the Tusk Studio web server"""
    from tusk.core.deps import require_feature
    require_feature("studio")

    from tusk.core.config import get_config, update_config

    config = get_config()
    host = config.host
    port = config.port

    # Parse optional arguments
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] in ("--host",) and i + 1 < len(args):
            host = args[i + 1]
            i += 2
        elif args[i] in ("--port", "-p") and i + 1 < len(args):
            port = int(args[i + 1])
            i += 2
        elif args[i] == "--pg-bin-path" and i + 1 < len(args):
            pg_path = args[i + 1]
            # Validate the path
            pg_dump = Path(pg_path) / "pg_dump"
            if not pg_dump.exists():
                print(f"Warning: pg_dump not found at {pg_dump}")
            else:
                print(f"Using PostgreSQL binaries from: {pg_path}")
            update_config(pg_bin_path=pg_path)
            i += 2
        else:
            i += 1

    print(f"Starting Tusk Studio at http://{host}:{port}")

    subprocess.run([
        sys.executable, "-m", "granian",
        "--interface", "asgi",
        "--host", host,
        "--port", str(port),
        "tusk.studio.app:app"
    ])


def handle_config():
    """Handle config subcommands"""
    from tusk.core.config import get_config, update_config, CONFIG_FILE

    args = sys.argv[2:]

    if not args or args[0] == "show":
        # Show current config
        config = get_config()
        print(f"Configuration file: {CONFIG_FILE}")
        print()
        print("[postgresql]")
        print(f"  bin_path = {config.pg_bin_path or '(auto-detect)'}")
        print()
        print("[server]")
        print(f"  host = {config.host}")
        print(f"  port = {config.port}")
        print()
        print("[ui]")
        print(f"  theme = {config.theme}")
        print(f"  editor_font_size = {config.editor_font_size}")

        # Show detected pg_dump path (only if postgres feature is available)
        try:
            from tusk.admin.backup import get_pg_dump_path, get_psql_path
            print()
            print("Detected PostgreSQL binaries:")
            print(f"  pg_dump = {get_pg_dump_path()}")
            print(f"  psql = {get_psql_path()}")
        except ImportError:
            pass  # postgres feature not installed

    elif args[0] == "set" and len(args) >= 3:
        key = args[1]
        value = args[2]

        # Map user-friendly keys to config keys
        key_map = {
            "pg_bin_path": "pg_bin_path",
            "postgresql.bin_path": "pg_bin_path",
            "host": "host",
            "server.host": "host",
            "port": "port",
            "server.port": "port",
            "theme": "theme",
            "ui.theme": "theme",
            "editor_font_size": "editor_font_size",
            "ui.editor_font_size": "editor_font_size",
        }

        config_key = key_map.get(key)
        if not config_key:
            print(f"Unknown config key: {key}")
            print(f"Valid keys: {', '.join(key_map.keys())}")
            sys.exit(1)

        # Convert value types
        if config_key in ("port", "editor_font_size"):
            value = int(value)

        update_config(**{config_key: value})
        print(f"Set {key} = {value}")
        print(f"Saved to {CONFIG_FILE}")

    else:
        print("Usage: tusk config [show|set KEY VALUE]")
        sys.exit(1)


def start_scheduler():
    """Start the cluster scheduler"""
    from tusk.core.deps import require_feature
    require_feature("cluster")

    host = "0.0.0.0"
    port = 8814

    # Parse arguments
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] == "--host" and i + 1 < len(args):
            host = args[i + 1]
            i += 2
        elif args[i] in ("--port", "-p") and i + 1 < len(args):
            port = int(args[i + 1])
            i += 2
        else:
            i += 1

    print(f"Starting Tusk Scheduler on {host}:{port}")

    from tusk.cluster.scheduler import Scheduler
    scheduler = Scheduler(host=host, port=port)
    try:
        scheduler.serve()
    except KeyboardInterrupt:
        print("\nShutting down scheduler...")
        scheduler.shutdown()


def start_worker():
    """Start a cluster worker"""
    from tusk.core.deps import require_feature
    require_feature("cluster")

    scheduler_host = "localhost"
    scheduler_port = 8814
    host = "0.0.0.0"
    port = 8815

    # Parse arguments
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] == "--scheduler" and i + 1 < len(args):
            addr = args[i + 1]
            if ":" in addr:
                scheduler_host, scheduler_port = addr.split(":")
                scheduler_port = int(scheduler_port)
            else:
                scheduler_host = addr
            i += 2
        elif args[i] == "--host" and i + 1 < len(args):
            host = args[i + 1]
            i += 2
        elif args[i] in ("--port", "-p") and i + 1 < len(args):
            port = int(args[i + 1])
            i += 2
        else:
            i += 1

    print(f"Starting Tusk Worker on {host}:{port}")
    print(f"Connecting to scheduler at {scheduler_host}:{scheduler_port}")

    from tusk.cluster.worker import Worker
    worker = Worker(
        scheduler_host=scheduler_host,
        scheduler_port=scheduler_port,
        host=host,
        port=port
    )
    try:
        worker.serve()
    except KeyboardInterrupt:
        print("\nShutting down worker...")
        worker.shutdown()


def start_cluster_dev():
    """Start local cluster with scheduler + workers for development"""
    from tusk.core.deps import require_feature
    require_feature("cluster")

    import multiprocessing
    import time
    import os

    num_workers = 3

    # Parse arguments
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] in ("--workers", "-w") and i + 1 < len(args):
            num_workers = int(args[i + 1])
            i += 2
        else:
            i += 1

    print(f"Starting Tusk Cluster (dev mode)")
    print(f"  Scheduler: localhost:8814")
    print(f"  Workers: {num_workers}")

    processes = []

    def run_scheduler():
        from tusk.cluster.scheduler import Scheduler
        scheduler = Scheduler(host="0.0.0.0", port=8814)
        scheduler.serve()

    def run_worker(worker_port):
        from tusk.cluster.worker import Worker
        worker = Worker(
            scheduler_host="localhost",
            scheduler_port=8814,
            host="0.0.0.0",
            port=worker_port
        )
        worker.serve()

    # Start scheduler
    p_scheduler = multiprocessing.Process(target=run_scheduler)
    p_scheduler.start()
    processes.append(p_scheduler)

    # Wait for scheduler to start
    time.sleep(1)

    # Start workers
    for i in range(num_workers):
        worker_port = 8815 + i
        p = multiprocessing.Process(target=run_worker, args=(worker_port,))
        p.start()
        processes.append(p)
        print(f"  Worker {i+1}: localhost:{worker_port}")

    print("\nCluster started. Press Ctrl+C to stop.")

    try:
        # Wait for all processes
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\nShutting down cluster...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join(timeout=5)
        print("Cluster stopped.")


def handle_users():
    """Handle user management commands"""
    from tusk.core.deps import require_feature
    require_feature("studio")

    from tusk.core.auth import (
        list_users, create_user, get_user_by_username,
        delete_user, update_password, init_auth_db
    )

    args = sys.argv[2:]

    if not args or args[0] == "list":
        init_auth_db()
        users = list_users()
        if not users:
            print("No users found")
            return

        print(f"{'Username':<20} {'Display Name':<25} {'Admin':<8} {'Active':<8}")
        print("-" * 65)
        for u in users:
            print(f"{u.username:<20} {(u.display_name or '-'):<25} {'Yes' if u.is_admin else 'No':<8} {'Yes' if u.is_active else 'No':<8}")

    elif args[0] == "create" and len(args) >= 2:
        username = args[1]
        is_admin = "--admin" in args

        import getpass
        password = getpass.getpass(f"Password for {username}: ")
        if not password:
            print("Password cannot be empty")
            sys.exit(1)

        confirm = getpass.getpass("Confirm password: ")
        if password != confirm:
            print("Passwords do not match")
            sys.exit(1)

        try:
            user = create_user(username=username, password=password, is_admin=is_admin)
            print(f"User '{username}' created successfully")
            if is_admin:
                print("  (Administrator)")
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    elif args[0] == "delete" and len(args) >= 2:
        username = args[1]
        user = get_user_by_username(username)
        if not user:
            print(f"User '{username}' not found")
            sys.exit(1)

        confirm = input(f"Delete user '{username}'? [y/N]: ")
        if confirm.lower() != 'y':
            print("Cancelled")
            return

        delete_user(user.id)
        print(f"User '{username}' deleted")

    elif args[0] == "reset-password" and len(args) >= 2:
        username = args[1]
        user = get_user_by_username(username)
        if not user:
            print(f"User '{username}' not found")
            sys.exit(1)

        import getpass
        password = getpass.getpass(f"New password for {username}: ")
        if not password:
            print("Password cannot be empty")
            sys.exit(1)

        confirm = getpass.getpass("Confirm password: ")
        if password != confirm:
            print("Passwords do not match")
            sys.exit(1)

        update_password(user.id, password)
        print(f"Password for '{username}' has been reset")

    else:
        print("Usage: tusk users [list|create|delete|reset-password] ...")
        sys.exit(1)


def handle_auth():
    """Handle auth configuration commands"""
    from tusk.core.deps import require_feature
    require_feature("studio")

    from tusk.core.config import get_config, update_config
    from tusk.core.auth import setup_default_groups, setup_admin_user, init_auth_db

    args = sys.argv[2:]

    if not args:
        print("Usage: tusk auth [init|enable|disable]")
        sys.exit(1)

    if args[0] == "init":
        config = get_config()
        if config.auth_mode != "multi":
            print("Auth mode is not enabled. Run 'tusk auth enable' first.")
            sys.exit(1)

        init_auth_db()
        setup_default_groups()
        print("Default groups created:")
        print("  - Administrators (all permissions)")
        print("  - Data Engineers")
        print("  - Analysts")
        print("  - Viewers")

        import getpass
        password = getpass.getpass("Set admin password (default: admin): ") or "admin"

        user = setup_admin_user(password=password)
        if user:
            print(f"\nAdmin user created: {user.username}")
        else:
            print("\nAdmin user already exists")

        print("\nAuth system initialized. Start studio to use.")

    elif args[0] == "enable":
        update_config(auth_mode="multi")
        print("Multi-user auth mode enabled")
        print("Run 'tusk auth init' to create admin user")

    elif args[0] == "disable":
        update_config(auth_mode="single")
        print("Auth disabled (single-user mode)")

    else:
        print("Usage: tusk auth [init|enable|disable]")
        sys.exit(1)


if __name__ == "__main__":
    main()
