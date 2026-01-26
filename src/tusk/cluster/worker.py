"""Worker for distributed query execution"""

import asyncio
import os
import socket
import time
from pathlib import Path
from uuid import uuid4

import psutil
import pyarrow as pa
import pyarrow.flight as flight
from datafusion import SessionContext

from tusk.core.logging import get_logger

log = get_logger("worker")


class FlightWorkerServer(flight.FlightServerBase):
    """Arrow Flight server for worker"""

    def __init__(self, worker: "Worker", location: str):
        super().__init__(location)
        self.worker = worker

    def do_get(self, context, ticket):
        """Handle query execution request"""
        sql = ticket.ticket.decode()
        log.info("Executing query", sql=sql[:100])

        try:
            table = self.worker.execute_query(sql)
            return flight.RecordBatchStream(table)
        except Exception as e:
            log.error("Query execution failed", error=str(e))
            raise

    def do_action(self, context, action):
        """Handle control actions"""
        action_type = action.type

        if action_type == "ping":
            yield flight.Result(b"pong")

        elif action_type == "status":
            cpu = psutil.cpu_percent()
            memory = psutil.Process().memory_info().rss / 1024 / 1024
            memory_percent = psutil.virtual_memory().percent
            status = f"{cpu}:{memory}:{memory_percent}"
            yield flight.Result(status.encode())

        else:
            yield flight.Result(b"unknown_action")


class Worker:
    """Distributed query worker using DataFusion"""

    def __init__(
        self,
        scheduler_host: str = "localhost",
        scheduler_port: int = 8814,
        host: str = "0.0.0.0",
        port: int = 8815,
        data_path: str | None = None,
    ):
        self.id = f"worker_{str(uuid4())[:6]}"
        self.scheduler_host = scheduler_host
        self.scheduler_port = scheduler_port
        self.host = host
        self.port = port
        self.data_path = Path(data_path).expanduser() if data_path else Path.home() / "data"

        # DataFusion context
        self.ctx = SessionContext()
        self._server: FlightWorkerServer | None = None
        self._running = False
        self._heartbeat_task: asyncio.Task | None = None

    def _get_local_ip(self) -> str:
        """Get local IP address for registration"""
        try:
            # Try to get external IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    def _register_with_scheduler(self) -> bool:
        """Register with the scheduler"""
        try:
            location = flight.Location.for_grpc_tcp(self.scheduler_host, self.scheduler_port)
            client = flight.FlightClient(location)

            # Get our address for registration
            local_ip = self._get_local_ip()
            if self.host == "0.0.0.0":
                register_host = local_ip
            else:
                register_host = self.host

            # Send registration action
            action = flight.Action(
                "register",
                f"{self.id}:{register_host}:{self.port}".encode()
            )
            list(client.do_action(action))

            log.info("Registered with scheduler",
                     scheduler=f"{self.scheduler_host}:{self.scheduler_port}",
                     worker_id=self.id)
            return True

        except Exception as e:
            log.error("Failed to register with scheduler", error=str(e))
            return False

    def _unregister_from_scheduler(self) -> None:
        """Unregister from the scheduler"""
        try:
            location = flight.Location.for_grpc_tcp(self.scheduler_host, self.scheduler_port)
            client = flight.FlightClient(location)

            action = flight.Action("unregister", self.id.encode())
            list(client.do_action(action))

            log.info("Unregistered from scheduler", worker_id=self.id)

        except Exception as e:
            log.warning("Failed to unregister from scheduler", error=str(e))

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to scheduler"""
        while self._running:
            try:
                await asyncio.sleep(5)

                if not self._running:
                    break

                cpu = psutil.cpu_percent()
                memory = psutil.Process().memory_info().rss / 1024 / 1024
                memory_percent = psutil.virtual_memory().percent

                location = flight.Location.for_grpc_tcp(self.scheduler_host, self.scheduler_port)
                client = flight.FlightClient(location)

                action = flight.Action(
                    "heartbeat",
                    f"{self.id}:{cpu}:{memory}:{memory_percent}".encode()
                )
                list(client.do_action(action))

                log.debug("Heartbeat sent", cpu=cpu, memory_mb=memory)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("Heartbeat failed", error=str(e))

    def _register_parquet_files(self, sql: str) -> None:
        """Auto-register parquet files mentioned in the query"""
        # Look for read_parquet('path') patterns
        import re
        pattern = r"read_parquet\s*\(\s*['\"]([^'\"]+)['\"]\s*\)"
        matches = re.findall(pattern, sql, re.IGNORECASE)

        for path in matches:
            p = Path(path).expanduser()
            if p.exists():
                table_name = p.stem
                try:
                    self.ctx.register_parquet(table_name, str(p))
                    log.debug("Registered parquet file", path=str(p), table=table_name)
                except Exception as e:
                    log.warning("Failed to register parquet", path=str(p), error=str(e))

    def execute_query(self, sql: str) -> pa.Table:
        """Execute a query using DataFusion"""
        start = time.time()

        # Auto-register parquet files
        self._register_parquet_files(sql)

        try:
            # Execute with DataFusion
            df = self.ctx.sql(sql)
            table = df.to_arrow_table()

            elapsed = time.time() - start
            log.info("Query executed",
                     rows=table.num_rows,
                     cols=table.num_columns,
                     time_ms=round(elapsed * 1000, 2))

            return table

        except Exception as e:
            log.error("Query execution failed", error=str(e))
            raise

    def serve(self) -> None:
        """Start the worker server (blocking)"""
        # Register with scheduler first
        if not self._register_with_scheduler():
            log.warning("Could not register with scheduler, starting anyway...")

        # Start Flight server
        location = f"grpc://{self.host}:{self.port}"
        self._server = FlightWorkerServer(self, location)
        self._running = True

        log.info("Worker starting", worker_id=self.id, host=self.host, port=self.port)
        print(f"Worker {self.id} listening on {self.host}:{self.port}")

        # Start heartbeat in background
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._heartbeat_task = loop.create_task(self._heartbeat_loop())

        try:
            self._server.serve()
        finally:
            self._running = False
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            self._unregister_from_scheduler()

    def shutdown(self) -> None:
        """Shutdown the worker"""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._server:
            self._server.shutdown()
        self._unregister_from_scheduler()
        log.info("Worker shutdown", worker_id=self.id)
