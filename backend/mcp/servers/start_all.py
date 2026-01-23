#!/usr/bin/env python3
"""
Start All MCP Servers

WHY: Provides a single entry point to start all MCP servers for development.
In production, each server runs as a separate container.

RUN: python -m backend.mcp.servers.start_all
"""
import asyncio
import subprocess
import os
import sys
import signal
from typing import List, Dict

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

import structlog

logger = structlog.get_logger()

# MCP Server configurations - Shared by IT Service Agent and Data Agent
MCP_SERVERS = [
    # IT Service Agent Servers
    {
        "name": "servicenow-mcp",
        "module": "mcp_servers.servicenow_mcp.server",
        "cwd": os.path.join(os.path.dirname(__file__), "..", "..", ".."),
        "env": {
            "SNOW_INSTANCE_URL": os.getenv("SNOW_INSTANCE_URL", ""),
            "SNOW_USERNAME": os.getenv("SNOW_USERNAME", ""),
            "SNOW_PASSWORD": os.getenv("SNOW_PASSWORD", ""),
            "METRICS_PORT": "8091"
        }
    },
    {
        "name": "github-mcp",
        "module": "mcp_servers.github_mcp.server",
        "cwd": os.path.join(os.path.dirname(__file__), "..", "..", ".."),
        "env": {
            "GITHUB_TOKEN": os.getenv("GITHUB_TOKEN", ""),
            "METRICS_PORT": "8092"
        }
    },
    {
        "name": "gcp-mcp",
        "module": "mcp_servers.gcp_mcp.server",
        "cwd": os.path.join(os.path.dirname(__file__), "..", "..", ".."),
        "env": {
            "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
            "GCP_PROJECT_ID": os.getenv("GCP_PROJECT_ID", ""),
            "METRICS_PORT": "8093"
        }
    },
    {
        "name": "jira-mcp",
        "module": "mcp_servers.jira_mcp.server",
        "cwd": os.path.join(os.path.dirname(__file__), "..", "..", ".."),
        "env": {
            "JIRA_URL": os.getenv("JIRA_URL", ""),
            "JIRA_EMAIL": os.getenv("JIRA_EMAIL", ""),
            "JIRA_API_TOKEN": os.getenv("JIRA_API_TOKEN", ""),
            "METRICS_PORT": "8094"
        }
    },
    {
        "name": "rag-mcp",
        "module": "backend.mcp.servers.rag_server",
        "cwd": os.path.join(os.path.dirname(__file__), "..", "..", ".."),
        "env": {
            "WEAVIATE_URL": os.getenv("WEAVIATE_URL", "http://localhost:8080"),
            "NEO4J_URI": os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            "METRICS_PORT": "8095"
        }
    },
    # Shared Servers (IT Service + Data Agent)
    {
        "name": "gcs-mcp",
        "module": "backend.mcp.servers.gcs_server",
        "cwd": os.path.join(os.path.dirname(__file__), "..", "..", ".."),
        "port": 8011,
        "env": {
            "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
            "GCP_PROJECT_ID": os.getenv("GCP_PROJECT_ID", ""),
            "METRICS_PORT": "8096"
        }
    },
    {
        "name": "iceberg-mcp",
        "module": "backend.mcp.servers.iceberg_server",
        "cwd": os.path.join(os.path.dirname(__file__), "..", "..", ".."),
        "port": 8012,
        "env": {
            "ICEBERG_CATALOG_URL": os.getenv("ICEBERG_CATALOG_URL", "http://localhost:8181"),
            "METRICS_PORT": "8097"
        }
    },
    {
        "name": "llm-mcp",
        "module": "backend.mcp.servers.llm_server",
        "cwd": os.path.join(os.path.dirname(__file__), "..", "..", ".."),
        "port": 8013,
        "env": {
            "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY", ""),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", ""),
            "METRICS_PORT": "8098"
        }
    },
]


class MCPServerManager:
    """
    Manages multiple MCP server processes.

    WHY:
    - Starts all servers in parallel
    - Monitors health
    - Clean shutdown on SIGTERM/SIGINT
    """

    def __init__(self):
        self.processes: Dict[str, subprocess.Popen] = {}
        self._shutdown = False

    def start_server(self, config: Dict) -> subprocess.Popen:
        """Start a single MCP server"""
        env = os.environ.copy()
        env.update(config.get("env", {}))

        process = subprocess.Popen(
            [sys.executable, "-m", config["module"]],
            cwd=config.get("cwd", "."),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        logger.info(
            "mcp_server_started",
            name=config["name"],
            pid=process.pid,
            module=config["module"]
        )

        return process

    def start_all(self):
        """Start all configured MCP servers"""
        logger.info("starting_all_mcp_servers", count=len(MCP_SERVERS))

        for config in MCP_SERVERS:
            try:
                process = self.start_server(config)
                self.processes[config["name"]] = process
            except Exception as e:
                logger.error(
                    "mcp_server_start_failed",
                    name=config["name"],
                    error=str(e)
                )

        logger.info("all_mcp_servers_started", running=list(self.processes.keys()))

    def stop_all(self):
        """Stop all running MCP servers"""
        logger.info("stopping_all_mcp_servers")
        self._shutdown = True

        for name, process in self.processes.items():
            try:
                process.terminate()
                process.wait(timeout=5)
                logger.info("mcp_server_stopped", name=name)
            except subprocess.TimeoutExpired:
                process.kill()
                logger.warning("mcp_server_killed", name=name)
            except Exception as e:
                logger.error("mcp_server_stop_failed", name=name, error=str(e))

    async def monitor(self):
        """Monitor server health and restart if needed"""
        while not self._shutdown:
            for name, process in list(self.processes.items()):
                if process.poll() is not None:
                    # Process died, restart it
                    logger.warning("mcp_server_died", name=name, returncode=process.returncode)

                    # Find config and restart
                    for config in MCP_SERVERS:
                        if config["name"] == name:
                            try:
                                new_process = self.start_server(config)
                                self.processes[name] = new_process
                                logger.info("mcp_server_restarted", name=name)
                            except Exception as e:
                                logger.error("mcp_server_restart_failed", name=name, error=str(e))
                            break

            await asyncio.sleep(10)  # Check every 10 seconds


async def main():
    """Main entry point"""
    manager = MCPServerManager()

    # Handle shutdown signals
    def shutdown_handler(signum, frame):
        logger.info("shutdown_signal_received", signal=signum)
        manager.stop_all()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    # Start all servers
    manager.start_all()

    # Monitor until shutdown
    try:
        await manager.monitor()
    except KeyboardInterrupt:
        manager.stop_all()


if __name__ == "__main__":
    print("Starting all MCP servers...")
    print("Press Ctrl+C to stop")
    asyncio.run(main())
