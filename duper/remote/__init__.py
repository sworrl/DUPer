"""Remote connectivity modules for DUPer."""

from duper.remote.agent import AgentClient, AgentResponse, connect_to_agent
from duper.remote.deploy import (
    Deployer,
    DeploymentOptions,
    DeploymentResult,
    deploy_to_host,
)
from duper.remote.ssh import (
    SSHConnectionInfo,
    SSHManager,
    SSHResult,
    create_ssh_connection,
    parse_ssh_target,
)

__all__ = [
    # Agent
    "AgentClient",
    "AgentResponse",
    "connect_to_agent",
    # SSH
    "SSHManager",
    "SSHConnectionInfo",
    "SSHResult",
    "create_ssh_connection",
    "parse_ssh_target",
    # Deploy
    "Deployer",
    "DeploymentOptions",
    "DeploymentResult",
    "deploy_to_host",
]
