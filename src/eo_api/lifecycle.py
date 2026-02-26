"""Application lifespan: Prefect server bootstrap and flow runner."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI


async def _serve_flows() -> None:
    """Register Prefect deployments and start a runner to execute them."""
    from prefect.runner import Runner

    from eo_api.prefect_flows.flows import ALL_FLOWS

    runner = Runner()
    for fl in ALL_FLOWS:
        await runner.aadd_flow(fl, name=fl.name)
    await runner.start()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start Prefect server, then register and serve pipeline deployments."""
    from eo_api.routers import prefect

    # Mounted sub-apps don't get their lifespans called automatically,
    # so we trigger the Prefect server's lifespan here to initialize
    # the database, docket, and background workers.
    prefect_app = prefect.app
    async with prefect_app.router.lifespan_context(prefect_app):
        task = asyncio.create_task(_serve_flows())
        yield
        task.cancel()
