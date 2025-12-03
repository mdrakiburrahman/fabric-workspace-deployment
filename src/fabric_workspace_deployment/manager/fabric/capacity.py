# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import json
import logging

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.operations.operation_interfaces import CapacityManager, CommonParams, FabricCapacityInfo, FabricCapacityParams


class FabricCapacityManager(CapacityManager):
    """Concrete implementation of CapacityManager for Microsoft Fabric."""

    def __init__(self, common_params: CommonParams, az_cli: AzCli, fabric_cli: FabricCli):
        """
        Initialize the Fabric capacity manager.
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.fabric_cli = fabric_cli
        self.logger = logging.getLogger(__name__)

    async def execute(self) -> None:
        self.logger.info("Executing FabricCapacityManager")
        tasks = []
        for workspace in self.common_params.fabric.workspaces:
            task = asyncio.create_task(self.reconcile(workspace.capacity), name=f"reconcile-capacity-{workspace.capacity.name}")
            tasks.append(task)

        if tasks:
            self.logger.info(f"Executing capacity reconciliation for {len(tasks)} workspaces in parallel")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    workspace_name = self.common_params.fabric.workspaces[i].capacity.name
                    error_msg = f"Failed to reconcile capacity '{workspace_name}': {result}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)

            if errors:
                error = f"Failed to reconcile some capacities: {'; '.join(errors)}"
                raise Exception(error)
        else:
            self.logger.info("No workspaces found to reconcile")

        self.logger.info("Finished executing FabricCapacityManager")

    async def reconcile(self, capacity_params: FabricCapacityParams) -> None:
        self.logger.info(f"Reconciling capacity: {capacity_params.name}")
        capacity_exists = await self.exists(capacity_params)

        if not capacity_exists:
            self.logger.info(f"Capacity '{capacity_params.name}' does not exist. Creating.")
            await self.create(capacity_params)
            self.logger.info(f"Successfully created capacity '{capacity_params.name}'")
        else:
            self.logger.info(f"Capacity '{capacity_params.name}' already exists")

        capacity_info = await self.get(capacity_params)

        if capacity_info.sku_name != capacity_params.sku:
            self.logger.info(f"Capacity '{capacity_params.name}' SKU mismatch. Current: {capacity_info.sku_name}, Desired: {capacity_params.sku}. Scaling.")  # noqa: E501
            await self.scale(capacity_params)
            self.logger.info(f"Successfully scaled capacity '{capacity_params.name}' to {capacity_params.sku}")
        else:
            self.logger.info(f"Capacity '{capacity_params.name}' SKU is already correct: {capacity_params.sku}")

        if capacity_info.state.lower() != "active":
            self.logger.info(f"Capacity '{capacity_params.name}' is not active (current state: {capacity_info.state}). Starting.")
            await self.start(capacity_params)
            self.logger.info(f"Successfully started capacity '{capacity_params.name}'")
        else:
            self.logger.info(f"Capacity '{capacity_params.name}' is already active")

        current_admins = set(capacity_info.administrators)
        expected_admins = set(capacity_params.administrators)
        if current_admins != expected_admins:
            self.logger.info(f"Capacity '{capacity_params.name}' administrators mismatch. Current: {current_admins}, Expected: {expected_admins}. Updating administrators.")  # noqa: E501
            await self.set(capacity_params, "properties.administration.members", json.dumps(capacity_params.administrators))
            self.logger.info(f"Successfully updated administrators for capacity '{capacity_params.name}'")
        else:
            self.logger.info(f"Capacity '{capacity_params.name}' administrators are already correct")

        self.logger.info(f"Completed reconciliation for capacity: {capacity_params.name}")

    async def exists(self, capacity_params: FabricCapacityParams) -> bool:
        output = self.fabric_cli.run_command(f"exists .capacities/{capacity_params.name}.Capacity")
        return output.strip().lstrip("* ").strip().lower() == "true"

    async def get(self, capacity_params: FabricCapacityParams) -> FabricCapacityInfo:
        output = self.fabric_cli.run_command(f"get .capacities/{capacity_params.name}.Capacity -q .")
        capacity_data = json.loads(output.strip())
        properties = capacity_data.get("properties", {})
        sku = capacity_data.get("sku", {})
        administration = properties.get("administration", {})
        return FabricCapacityInfo(
            id=capacity_data.get("id", ""),
            name=capacity_data.get("name", ""),
            type=capacity_data.get("type", ""),
            location=capacity_data.get("location", ""),
            sku_name=sku.get("name", ""),
            sku_tier=sku.get("tier", ""),
            tags=capacity_data.get("tags", {}),
            fabric_id=capacity_data.get("fabricId", ""),
            provisioning_state=properties.get("provisioningState", ""),
            state=properties.get("state", ""),
            administrators=administration.get("members", []),
        )

    async def create(self, capacity_params: FabricCapacityParams) -> None:
        """
        Note:

          - Fabric CLI creation only supports a single administrator, we
            reconcile via az after creation:

            >>> https://www.azurecitadel.com/fabric/capacity/#capacity-admins
        """
        params = []
        params.append(f"sku={capacity_params.sku}")
        params.append(f"location={self.common_params.arm.location}")
        params.append(f"resourcegroup={self.common_params.arm.resource_group}")
        params.append(f"subscriptionid={self.common_params.arm.subscription_id}")
        params.append(f"admin={capacity_params.administrators[0]}")

        params_str = ",".join(params)
        self.fabric_cli.run_command(f"create .capacities/{capacity_params.name}.Capacity -P {params_str}")

        await self.set(capacity_params, "properties.administration.members", json.dumps(capacity_params.administrators))

    async def remove(self, capacity_params: FabricCapacityParams) -> None:
        self.fabric_cli.run_command(f"rm .capacities/{capacity_params.name}.Capacity -f")

    async def scale(self, capacity_params: FabricCapacityParams) -> None:
        if hasattr(capacity_params, "sku") and capacity_params.sku:
            await self.set(capacity_params, "sku.name", capacity_params.sku)

    async def start(self, capacity_params: FabricCapacityParams) -> None:
        self.fabric_cli.run_command(f"start .capacities/{capacity_params.name}.Capacity -f")

    async def stop(self, capacity_params: FabricCapacityParams) -> None:
        self.fabric_cli.run_command(f"stop .capacities/{capacity_params.name}.Capacity -f")

    async def set(self, capacity_params: FabricCapacityParams, property_path: str, value: str) -> None:
        available_queries = ["sku.name", "properties.administration.members"]
        if property_path not in available_queries:
            error = f"Invalid query '{property_path}'. Available queries: {', '.join(available_queries)}"
            raise ValueError(error)
        resource_id = f"/subscriptions/{self.common_params.arm.subscription_id}/resourceGroups/{self.common_params.arm.resource_group}/providers/Microsoft.Fabric/capacities/{capacity_params.name}"  # noqa: E501
        self.az_cli.run(["resource", "update", "--ids", resource_id, "--set", f"{property_path}={value}", "--latest-include-preview"])

    async def assign(self, capacity_params: "FabricCapacityParams", workspace_name: str) -> None:
        self.fabric_cli.run_command(f"assign .capacities/{capacity_params.name}.Capacity -W {workspace_name}.Workspace -f")

    async def unassign(self, capacity_params: "FabricCapacityParams", workspace_name: str) -> None:
        self.fabric_cli.run_command(f"unassign .capacities/{capacity_params.name}.Capacity -W {workspace_name}.Workspace -f")
