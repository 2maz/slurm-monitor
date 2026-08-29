from argparse import ArgumentParser
import logging

import httpx
from fastmcp import FastMCP

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.app_settings import AppSettings

logger = logging.getLogger(__name__)

class MCPParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        app_settings = AppSettings.get_instance()
        default_mcp_port = app_settings.port + 1

        parser.description = "Start a Model Context Protocol (MCP) server that proxies " \
             "an already-running slurm-monitor RESTAPI (see 'slurm-monitor restapi') as MCP tools"

        parser.add_argument("--api-url", type=str,
                            default=f"http://localhost:{app_settings.port}/api/v2",
                            help="Base URL of the running RESTAPI to proxy, "
                                 f"default is http://localhost:{app_settings.port}/api/v2"
        )

        parser.add_argument("--transport", type=str,
                            choices=["stdio", "http", "sse", "streamable-http"],
                            default="http",
                            help="MCP transport to use, default is 'http'"
        )

        parser.add_argument("--host", type=str,
                            default=app_settings.host,
                            help=f"Set the MCP server's own host (http/sse/streamable-http transports only), "
                                 f"default is {app_settings.host}"
        )

        parser.add_argument("--port", type=int,
                            default=default_mcp_port,
                            help=f"Set the MCP server's own listen port (http/sse/streamable-http transports "
                                 f"only), default is one port higher than the restapi, currently {default_mcp_port}"
        )

        parser.add_argument("--path", type=str,
                            default=None,
                            help="Set the MCP endpoint path (http/sse/streamable-http transports only)"
        )

        parser.add_argument("--bearer-token", type=str,
                            default=None,
                            help="Bearer token to authenticate against the RESTAPI, "
                                 "required only if the RESTAPI has OAuth enabled"
        )

    def execute(self, args):
        super().execute(args)

        headers = {}
        if args.bearer_token:
            headers["Authorization"] = f"Bearer {args.bearer_token}"

        # Fetch the live OpenAPI schema from the running RESTAPI instance,
        # rather than importing api_v2_app and calling .openapi() locally,
        # so this always reflects whatever that instance is actually
        # serving (including a remote or differently-versioned one).
        openapi_url = f"{args.api_url.rstrip('/')}/openapi.json"
        try:
            response = httpx.get(openapi_url, headers=headers, timeout=10)
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise RuntimeError(
                f"Could not reach the RESTAPI's OpenAPI schema at '{openapi_url}' - "
                f"is 'slurm-monitor restapi' running there? ({e})"
            ) from e

        client = httpx.AsyncClient(base_url=args.api_url, headers=headers)

        mcp = FastMCP.from_openapi(
            openapi_spec=response.json(),
            client=client,
            name="slurm-monitor",
        )

        if args.transport == "stdio":
            mcp.run(transport="stdio")
        else:
            mcp.run(transport=args.transport, host=args.host, port=args.port, path=args.path)
