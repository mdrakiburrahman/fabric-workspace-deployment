#!/usr/bin/env python3
"""
Fabric Workspace Deployment.
"""

import argparse
import asyncio
import logging
from datetime import datetime

from fabric_workspace_deployment.operations.operation_interfaces import OperationParams
from fabric_workspace_deployment.operations.operators import CentralOperator
import os

# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #


def setup_logging() -> str:
    """
    Configure logging with timestamp-based filename.

    Returns:
        str: The log filename that was created.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # noqa: DTZ005
    log_filename = f"app_{timestamp}.log"
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
    )
    return log_filename


def dump_env_vars() -> None:
    """
    Log all environment variables for debugging purposes.
    """
    for key, value in sorted(os.environ.items()):
        logging.debug(f"{key}={value}")


def parse_config() -> OperationParams:
    """
    Parse command line arguments.

    Returns:
        OperationParams: The parsed operation parameters.
    """
    parser = argparse.ArgumentParser(description="Deploys Fabric Workspace.")
    parser.add_argument("--config-file-absolute-path", type=str, help="Absolute path to the configuration file.")
    parser.add_argument("--operation", type=str, help="The operation to execute.")
    args = parser.parse_args()

    logging.info(f"Config file absolute path: {args.config_file_absolute_path}")
    logging.info(f"Operation: {args.operation}")

    operation_params = OperationParams(args.config_file_absolute_path, args.operation, replace_placeholders=True)
    if operation_params.validate():
        logging.info("Configuration validation passed")
    else:
        logging.error("Configuration validation failed")
        error_message = "Invalid configuration parameters."
        raise ValueError(error_message)

    return operation_params


# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #


async def async_main() -> None:
    """
    Async entry point.
    """
    logging.info(f"Starting Fabric Deployer with logs at: {setup_logging()}.")
    dump_env_vars()
    operation_params = parse_config()
    await CentralOperator(operation_params).execute()
    logging.info("Fabric Deployment complete.")


def main() -> None:
    """
    Synchronous entry point for the CLI.
    """
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
