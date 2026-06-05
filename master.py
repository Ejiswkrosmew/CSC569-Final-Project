"""
Master server

- namespace: path -> file or directory metadata
- file -> ordered chunk handles
- chunk handle -> known replica locations
- chunkserver registry + heartbeat updates sent by chunkservers via network messages (in shared.py)

The master should be started first. Chunkservers then register
themselves with the master and continue sending heartbeat messages
"""



from __future__ import annotations

import subprocess
import socket
import struct
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path, PurePosixPath
# useful for pathing utils

from typing import Iterable

from shared import (
    ChunkServRequest,
    ChunkHB,
    ChunkLocReply,
    ChunkLocRequest,
    ChunkRegistration,
    CreateRequest,
    StatusReply,
    formats,
    from_bytes,
)


ChunkHandle = int
ChunkServerId = str

CHUNKSERVER_CREATE_CHUNK = 1
STATUS_OK = 1
STATUS_ERROR = 0

class NodeType(str, Enum):
    FILE = "file"
    DIRECTORY = "directory"


@dataclass
class ChunkLocation:
    # Network identity for one chunk replica

    server_id: ChunkServerId
    host: str
    port: int


@dataclass
class ChunkMetadata:
    # Master-side metadata for a chunk

    handle: ChunkHandle
    version: int = 1
    replicas: dict[ChunkServerId, ChunkLocation] = field(default_factory=dict)


@dataclass
class NamespaceNode:
    # A file or directory entry in the namespace.

    path: str
    node_type: NodeType
    permissions: str = "rw"
    chunk_handles: list[ChunkHandle] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)


@dataclass
class ChunkServerInfo:
    # State the master tracks for each chunkserver.

    server_id: ChunkServerId
    host: str
    port: int
    chunk_handles: set[ChunkHandle] = field(default_factory=set)
    last_heartbeat: float = field(default_factory=time.time)
    is_alive: bool = True
    recovery_started: bool = False


@dataclass
class MasterLogEntry:
    # One event in the master's in-memory metadata/change log.

    timestamp: float
    event_type: str
    server_id: ChunkServerId | None = None
    chunk_handle: ChunkHandle | None = None
    detail: str = ""


class NamespaceLockManager:
    """
    GFS uses read locks on parent directories and write locks on the target
    path. Only using one global lock -- low concurrency but maximum consistency
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()

    def acquire_create_locks(self, path: str) -> threading.RLock:

        self._lock.acquire()
        return self._lock

    def acquire_read_locks(self, path: str) -> threading.RLock:

        self._lock.acquire()
        return self._lock

    def release(self, lock: threading.RLock) -> None:
        lock.release()


class MasterMetadataStore:
    # All in-memory master metadata

    def __init__(self, replication_factor: int = 1) -> None:
        # Replication factor = 1 since that chunkserver will replicate to each MPI rank's local chunk store
        self.replication_factor = replication_factor
        self.namespace: dict[str, NamespaceNode] = {
            "/": NamespaceNode(path="/", node_type=NodeType.DIRECTORY)
        }
        self.chunks: dict[ChunkHandle, ChunkMetadata] = {}
        self.chunkservers: dict[ChunkServerId, ChunkServerInfo] = {}
        self.log: list[MasterLogEntry] = []
        self.locks = NamespaceLockManager()
        self._metadata_lock = threading.RLock()

    def register_chunkserver(
        self,
        server_id: ChunkServerId,
        host: str,
        port: int,
        chunk_handles: Iterable[ChunkHandle] = (),
    ) -> ChunkServerInfo:
        # Add or refresh a chunkserver and merge its reported chunks."""

        with self._metadata_lock:
            server = self.chunkservers.get(server_id)
            if server is None:
                server = ChunkServerInfo(
                    server_id=server_id, host=host, port=port)
                self.chunkservers[server_id] = server

            server.host = host
            server.port = port
            server.chunk_handles = {
                handle for handle in chunk_handles if handle != 0}
            server.last_heartbeat = time.time()
            server.is_alive = True
            server.recovery_started = False

            self._merge_chunk_inventory(server)
            self.append_log(
                event_type="chunkserver_registered",
                server_id=server_id,
                detail=f"port={port}, chunks={sorted(server.chunk_handles)}",
            )
            return server

    def update_chunkserver_heartbeat(
        self,
        server_id: ChunkServerId,
        chunk_handles: Iterable[ChunkHandle],
    ) -> None:
        # Update chunkserver liveness and which chunks it holds from a heartbeat.

        with self._metadata_lock:
            server = self.chunkservers[server_id]
            server.chunk_handles = {
                handle for handle in chunk_handles if handle != 0}
            server.last_heartbeat = time.time()
            server.is_alive = True
            server.recovery_started = False
            self._merge_chunk_inventory(server)
            self.append_log(
                event_type="chunkserver_heartbeat",
                server_id=server_id,
                detail=f"chunks={sorted(server.chunk_handles)}",
            )

    def record_chunkserver_chunk(
        self,
        server_id: ChunkServerId,
        host: str,
        port: int,
        chunk_handle: ChunkHandle,
    ) -> None:
        # Record one chunk reported by a chunkserver heartbeat if chunkserver has log entry

        with self._metadata_lock:
            server = self.chunkservers.get(server_id)
            if server is None:
                server = ChunkServerInfo(
                    server_id=server_id, host=host, port=port)
                self.chunkservers[server_id] = server

            server.host = host
            server.port = port
            server.last_heartbeat = time.time()
            server.is_alive = True
            server.recovery_started = False
            if chunk_handle != 0:
                server.chunk_handles.add(chunk_handle)
            self._merge_chunk_inventory(server)
            self.append_log(
                event_type="chunkserver_chunk_reported",
                server_id=server_id,
                chunk_handle=chunk_handle,
            )

    def record_chunk_allocation(
        self,
        server: ChunkServerInfo,
        chunk_handle: ChunkHandle,
    ) -> None:
        # Record a successful master-requested chunk allocation.

        with self._metadata_lock:
            server.chunk_handles.add(chunk_handle)
            server.last_heartbeat = time.time()
            server.is_alive = True
            self._merge_chunk_inventory(server)
            self.append_log(
                event_type="chunk_allocated",
                server_id=server.server_id,
                chunk_handle=chunk_handle,
                detail=f"port={server.port}",
            )

    def append_log(
        self,
        event_type: str,
        server_id: ChunkServerId | None = None,
        chunk_handle: ChunkHandle | None = None,
        detail: str = "",
    ) -> None:
        self.log.append(
            MasterLogEntry(
                timestamp=time.time(),
                event_type=event_type,
                server_id=server_id,
                chunk_handle=chunk_handle,
                detail=detail,
            )
        )

    def mark_dead_chunkservers(
        self,
        heartbeat_timeout_seconds: float,
    ) -> list[ChunkServerInfo]:
        # Mark servers dead if they have not responded recently.
        # will be restarted outside here so we don't block liveness checking

        cutoff = time.time() - heartbeat_timeout_seconds
        newly_dead_servers: list[ChunkServerInfo] = []
        with self._metadata_lock:
            for server in self.chunkservers.values():
                if server.is_alive and server.last_heartbeat < cutoff:
                    server.is_alive = False
                    self.append_log(
                        event_type="chunkserver_marked_dead",
                        server_id=server.server_id,
                        detail=f"port={server.port}",
                    )

                if not server.is_alive and not server.recovery_started:
                    server.recovery_started = True
                    newly_dead_servers.append(server)

        return newly_dead_servers

    def clear_recovery_started(self, server_id: ChunkServerId) -> None:
        with self._metadata_lock:
            server = self.chunkservers.get(server_id)
            if server is not None and not server.is_alive:
                server.recovery_started = False

    def create_directory(self, path: str, permissions: str = "rw") -> NamespaceNode:
        # Create a directory entry after validating its parent exists.

        normalized = self._normalize_path(path)
        # Lock entire namespace
        lock = self.locks.acquire_create_locks(normalized)
        try:
            with self._metadata_lock:
                self._ensure_parent_directory_exists(normalized)
                if normalized in self.namespace:
                    raise ValueError(f"path already exists: {normalized}")

                node = NamespaceNode(
                    path=normalized,
                    node_type=NodeType.DIRECTORY,
                    permissions=permissions,
                )
                self.namespace[normalized] = node
                return node
        finally:
            self.locks.release(lock)

    def create_file(
        self,
        path: str,
        number_of_chunks: int,
        permissions: str = "rw",
    ) -> NamespaceNode:
       # Create a file entry and allocate chunk handles.

        if number_of_chunks < 0:
            raise ValueError("number_of_chunks must be non-negative")

        normalized = self._normalize_path(path)
        lock = self.locks.acquire_create_locks(normalized)
        try:
            with self._metadata_lock:
                self._ensure_parent_directory_exists(normalized)
                if normalized in self.namespace:
                    raise ValueError(f"path already exists: {normalized}")

                handles = [self._new_chunk_handle()
                           for _ in range(number_of_chunks)]
                for handle in handles:
                    self.chunks[handle] = ChunkMetadata(handle=handle)

                node = NamespaceNode(
                    path=normalized,
                    node_type=NodeType.FILE,
                    permissions=permissions,
                    chunk_handles=handles,
                )
                self.namespace[normalized] = node
                return node
        finally:
            self.locks.release(lock)

    def get_chunk_locations(self, path: str, chunk_index: int) -> tuple[ChunkHandle, list[ChunkLocation]]:
        """
        Return the handle and live replica locations for one file chunk.
        "give me chunk N of /some/file and the chunkservers that hold it."
        """

        normalized = self._normalize_path(path)
        lock = self.locks.acquire_read_locks(normalized)
        try:
            with self._metadata_lock:
                node = self.namespace.get(normalized)
                if node is None:
                    raise FileNotFoundError(normalized)
                if node.node_type != NodeType.FILE:
                    raise IsADirectoryError(normalized)
                if chunk_index < 0 or chunk_index >= len(node.chunk_handles):
                    raise IndexError(
                        f"chunk index out of range: {chunk_index}")

                handle = node.chunk_handles[chunk_index]
                metadata = self.chunks[handle]
                live_locations = [
                    location
                    for location in metadata.replicas.values()
                    if self.chunkservers[location.server_id].is_alive
                ]
                return handle, live_locations
        finally:
            self.locks.release(lock)

    def choose_replica_targets(self) -> list[ChunkServerInfo]:
        """
        Choose chunkservers for a new chunk

        simple strategy: Pick live servers with the fewest chunks
        """

        with self._metadata_lock:
            live_servers = [
                server for server in self.chunkservers.values() if server.is_alive
            ]
            live_servers.sort(key=lambda server: len(server.chunk_handles))
            return live_servers[: self.replication_factor]

    def _merge_chunk_inventory(self, server: ChunkServerInfo) -> None:
        location = ChunkLocation(
            server_id=server.server_id,
            host=server.host,
            port=server.port,
        )
        for handle in server.chunk_handles:
            chunk = self.chunks.setdefault(
                handle, ChunkMetadata(handle=handle))
            chunk.replicas[server.server_id] = location

    def _ensure_parent_directory_exists(self, path: str) -> None:
        parent = str(PurePosixPath(path).parent)
        if parent == ".":
            parent = "/"
        node = self.namespace.get(parent)
        if node is None or node.node_type != NodeType.DIRECTORY:
            raise FileNotFoundError(
                f"parent directory does not exist: {parent}")

    @staticmethod
    def _normalize_path(path: str) -> str:
        normalized = str(PurePosixPath("/", path))
        if normalized != "/" and normalized.endswith("/"):
            normalized = normalized.rstrip("/")
        return normalized

    @staticmethod
    def _new_chunk_handle() -> ChunkHandle:
        # shared.py uses "I" which is 32-bit unsigned
        return uuid.uuid4().int & ((1 << 32) - 1)


class MasterService:
    """
    TCP master process.

    The master listens on one port. Each incoming connection should send one
    fixed-size struct message from shared.py. The master unpacks the first four
    bytes to get message type, sends to the matching handler, and
    sends a fixed-size reply if applicable
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8888,
        heartbeat_timeout_seconds: float = 15.0,
    ) -> None:
        self.host = host
        self.port = port
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.store = MasterMetadataStore()
        self._stop_event = threading.Event()
        self._server_socket: socket.socket | None = None
        self._liveness_thread: threading.Thread | None = None

    def start(self) -> None:
        # Start the master TCP listener and liveness monitor

        self._stop_event.clear()
        self._liveness_thread = threading.Thread(
            target=self.liveness_loop,
            name="master-liveness",
            daemon=True,
        )
        self._liveness_thread.start()
        self.start_tcp_server()

    def stop(self) -> None:
        # Stop background tasks.

        self._stop_event.set()
        if self._server_socket is not None:
            self._server_socket.close()
        if self._liveness_thread is not None:
            self._liveness_thread.join(timeout=2.0)

    def start_tcp_server(self) -> None:
        """
        Listen forever for client and chunkserver messages.

        This uses a thread per accepted connection so one slow client does not
        block registrations, chunk-location lookups, or heartbeat updates
        """

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self._server_socket = server_socket
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen()
            server_socket.settimeout(1.0)
            print(f"Master listening on {self.host}:{self.port}")

            while not self._stop_event.is_set():
                try:
                    connection, address = server_socket.accept()
                except TimeoutError:
                    continue
                except OSError:
                    break

                threading.Thread(
                    target=self.handle_connection,
                    args=(connection, address),
                    daemon=True,
                ).start()

    def handle_chunkserver_registration(
        self,
        registration: ChunkRegistration,
        host: str,
    ) -> ChunkServerInfo:
        server_id = self.server_id_for_port(registration.port)
        return self.store.register_chunkserver(
            server_id=server_id,
            host=host,
            port=registration.port,
            chunk_handles=registration.chunk_handles,
        )

    def handle_client_lookup(
        self,
        request: ChunkLocRequest,
    ) -> ChunkLocReply:
        path = self.decode_path(request.filepath)
        chunk_index = request.chunk_num
        handle, locations = self.store.get_chunk_locations(path, chunk_index)
        if not locations:
            raise LookupError(
                f"no live replicas for {path} chunk {chunk_index}")
        return ChunkLocReply(chunk_handle=handle, replica_port=locations[0].port)

    def handle_create_request(self, request: CreateRequest) -> StatusReply:
        path = self.decode_path(request.filepath)
        self.create_parent_directories(path)
        if request.chunks == 0:
            if path not in self.store.namespace:
                self.store.create_directory(path)
            self.store.append_log(event_type="directory_created", detail=path)
            return StatusReply(status=STATUS_OK)

        node = self.store.create_file(path, request.chunks)
        try:
            self.allocate_file_chunks(node)
        except Exception:
            return StatusReply(status=STATUS_ERROR)

        self.store.append_log(event_type="file_created", detail=path)
        return StatusReply(status=STATUS_OK)

    def handle_chunkserver_heartbeat(
        self,
        heartbeat: ChunkHB,
        host: str,
        fallback_port: int,
    ) -> StatusReply:
        """
        Update metadata from a chunkserver-sent heartbeat.
        """

        src_port = heartbeat.src_port or fallback_port
        server_id = self.server_id_for_port(src_port)
        self.store.record_chunkserver_chunk(
            server_id=server_id,
            host=host,
            port=src_port,
            chunk_handle=heartbeat.chunk_handle,
        )
        return StatusReply(status=1)

    def handle_connection(
        self,
        connection: socket.socket,
        address: tuple[str, int],
    ) -> None:
        with connection:
            while not self._stop_event.is_set():
                message = None
                try:
                    message = self.read_message(connection)
                    reply = self.dispatch_message(message, address)
                    if reply is not None:
                        connection.sendall(reply.to_bytes())
                        if self.is_client_request(message):
                            break
                except ConnectionError:
                    break
                except Exception as exc:
                    print(
                        f"Master failed to handle message from {address}: {exc}")
                    try:
                        connection.sendall(
                            StatusReply(status=STATUS_ERROR).to_bytes()
                        )
                    except OSError:
                        break
                    if message is None or self.is_client_request(message):
                        break

    def dispatch_message(self, message: object, address: tuple[str, int]):
        host, peer_port = address
        if isinstance(message, ChunkRegistration):
            self.handle_chunkserver_registration(message, host)
            return None

        if isinstance(message, ChunkHB):
            self.handle_chunkserver_heartbeat(message, host, peer_port)
            return None

        if isinstance(message, CreateRequest):
            return self.handle_create_request(message)

        if isinstance(message, ChunkLocRequest):
            return self.handle_client_lookup(message)

        raise ValueError(f"unsupported message type: {type(message).__name__}")

    def allocate_file_chunks(self, node: NamespaceNode) -> None:
        # Ask chunkservers to create every chunk for a new file

        for chunk_handle in node.chunk_handles:
            targets = self.store.choose_replica_targets()
            if not targets:
                raise RuntimeError("no live chunkservers available")

            target = targets[0]
            if not self.request_chunk_creation(target, chunk_handle):
                raise RuntimeError(
                    f"chunkserver {target.server_id} failed to create {chunk_handle}"
                )
            self.store.record_chunk_allocation(target, chunk_handle)

    def request_chunk_creation(
        self,
        server: ChunkServerInfo,
        chunk_handle: ChunkHandle,
    ) -> bool:
        # Send a create-chunk request to one chunkserver

        request = ChunkServRequest(
            req_type=CHUNKSERVER_CREATE_CHUNK,
            chunk_handle=chunk_handle,
            data=b"",
        )
        with socket.create_connection((server.host, server.port), timeout=2.0) as sock:
            sock.sendall(request.to_bytes())
            reply = self.read_message(sock)
        return isinstance(reply, StatusReply) and reply.status == STATUS_OK

    def read_message(self, connection: socket.socket) -> object:
        header = self.recv_exact(connection, 4)
        format_type = struct.unpack("!I", header)[0]
        if format_type not in formats:
            raise ValueError(f"unknown format type: {format_type}")

        message_size = struct.calcsize(formats[format_type])
        body = self.recv_exact(connection, message_size - 4)
        return from_bytes(header + body)

    @staticmethod
    def recv_exact(connection: socket.socket, byte_count: int) -> bytes:
        # Might not get all bytes in one call, so loop until we have them all -- we know the lenght from the struct format
        chunks = []
        bytes_read = 0
        while bytes_read < byte_count:
            chunk = connection.recv(byte_count - bytes_read)
            if not chunk:
                raise ConnectionError(
                    "connection closed before full message arrived")
            chunks.append(chunk)
            bytes_read += len(chunk)
        return b"".join(chunks)

    @staticmethod
    def is_client_request(message: object) -> bool:
        return isinstance(message, (CreateRequest, ChunkLocRequest))

    def liveness_loop(self) -> None:
        while not self._stop_event.is_set():
            dead_servers = self.store.mark_dead_chunkservers(
                self.heartbeat_timeout_seconds
            )
            for server in dead_servers:
                self.restart_dead_chunkserver(server)
            self._stop_event.wait(self.heartbeat_timeout_seconds / 2)

    def restart_dead_chunkserver(self, server: ChunkServerInfo) -> None:
        """
        Restart a dead primary chunkserver from its replica-created dump file.

        chunknew.py expects positional args:
            chunkserver_port master_port chunkdump_path
        """

        # path to dump file should be chunkdump-{port} in the same directory as master.py and chunknew.py
        workspace = Path(__file__).resolve().parent
        dump_file = workspace / f"chunkdump-{server.port}"

        if not dump_file.exists():
            print(
                f"Dead chunkserver {server.server_id} has no dump file at {dump_file}"
            )
            self.store.append_log(
                event_type="chunkserver_recovery_dump_missing",
                server_id=server.server_id,
                detail=str(dump_file),
            )
            self.store.clear_recovery_started(server.server_id)
            return

        command = [
            "mpiexec",
            "-n",
            "4",
            sys.executable,
            str(workspace / "chunknew.py"),
            str(server.port),
            str(self.port),
            str(dump_file),
        ]

        try:
            subprocess.Popen(command, cwd=workspace)
            print(
                f"Restarting {server.server_id} on port {server.port} from {dump_file}"
            )
            self.store.append_log(
                event_type="chunkserver_recovery_started",
                server_id=server.server_id,
                detail=" ".join(command),
            )
        except Exception as exc:
            print(f"Failed to restart {server.server_id}: {exc}")
            self.store.append_log(
                event_type="chunkserver_recovery_failed",
                server_id=server.server_id,
                detail=str(exc),
            )
            self.store.clear_recovery_started(server.server_id)

    @staticmethod
    def server_id_for_port(port: int) -> str:
        # label is cs-port
        return f"cs-{port}"

    @staticmethod
    def decode_path(raw_path: bytes) -> str:
        return raw_path.split(b"\0", 1)[0].decode("utf-8")

    def create_parent_directories(self, path: str) -> None:
        current = PurePosixPath("/")
        for part in PurePosixPath(path).parent.parts:
            if part == "/":
                continue
            current = current / part
            normalized = str(current)
            if normalized not in self.store.namespace:
                self.store.create_directory(normalized)

    def get_chunk_locations(
        self,
        path: str,
        chunk_index: int,
    ) -> tuple[ChunkHandle, list[ChunkLocation]]:
        # Helper
        return self.store.get_chunk_locations(path, chunk_index)


if __name__ == "__main__":
    master = MasterService()
    try:
        master.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        master.stop()
