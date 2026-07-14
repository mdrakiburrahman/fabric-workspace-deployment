#!/usr/bin/env python3
"""
Fabric Workspace Deployment.
"""

import argparse
import asyncio
import logging
import os
from datetime import datetime

from fabric_workspace_deployment.logging_config import setup_logging_from_config
from fabric_workspace_deployment.operations.operation_interfaces import OperationParams
from fabric_workspace_deployment.operations.operators import CentralOperator

# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #


def dump_env_vars() -> None:
    """
    Log all environment variables for debugging purposes.
    """
    for key, value in sorted(os.environ.items()):
        logging.debug(f"{key}={value}")


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser(description="Deploys Fabric Workspace.")
    parser.add_argument("--config-file-absolute-path", type=str, help="Absolute path to the configuration file.")
    parser.add_argument("--operation", type=str, help="The operation to execute.")
    return parser.parse_args()


def build_operation_params(args: argparse.Namespace) -> OperationParams:
    """
    Build and validate operation parameters from parsed arguments.

    Args:
        args: The parsed command line arguments.

    Returns:
        OperationParams: The validated operation parameters.

    Raises:
        ValueError: If the configuration is invalid.
    """
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
    args = parse_args()
    if not args.config_file_absolute_path:
        error_message = "The --config-file-absolute-path argument is required."
        raise ValueError(error_message)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # noqa: DTZ005
    log_dir = setup_logging_from_config(args.config_file_absolute_path, timestamp)
    logging.info(f"Starting Fabric Deployer with logs at: {log_dir}.")

    dump_env_vars()
    operation_params = build_operation_params(args)
    await CentralOperator(operation_params).execute()
    logging.info("Fabric Deployment complete.")


def main() -> None:
    """
    Synchronous entry point for the CLI.
    """
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
