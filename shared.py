import struct
from dataclasses import dataclass

formats = {
    0: "!III4096s",
    1: "!II?II",
    2: "!I50sI",
    3: "!II",
    4: "!I50sI",
    5: "!III",
    6: "!II10I"
}

# Request an operation on the chunkserver
@dataclass
class ChunkServRequest:
    format_type: int = 0
    req_type: int = 0
    chunk_handle: int = 0
    data: bytes = b""

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.req_type, self.chunk_handle, self.data)

# HB for chunkservers to send to the master
@dataclass
class ChunkHB:
    format_type: int = 1
    src_port: int = 0
    log_entry: bool = False
    chunk_handle: int = 0
    op_type: int = 0

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.src_port, self.log_entry, self.chunk_handle, self.op_type)

# Request to create a new file from the client to the master
@dataclass
class CreateRequest:
    format_type: int = 2
    filepath: bytes = b""
    chunks: int = 0

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.filepath, self.chunks)

# Generic status reply struct
@dataclass
class StatusReply:
    format_type: int = 3
    status: int = 0

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.status)

# Client request the location of a chunk from the master
@dataclass
class ChunkLocRequest:
    format_type: int = 4
    filepath: bytes = 0
    chunk_num: int = 0

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.filepath, self.chunk_num)

# Master’s reply of where a chunk is located at
@dataclass
class ChunkLocReply:
    format_type: int = 5
    chunk_handle: int = 0
    replica_port: int = 0

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.chunk_handle, self.replica_port)

# Initial chunkserver telling master about itself
@dataclass
class ChunkRegistration:
    format_type: int = 6
    port: int = 0
    chunk_handles: list[int] = None

    def to_bytes(self):
        handles = self.chunk_handles or []

        if len(handles) > 10:
            raise ValueError("chunk_handles can contain at most 10 handles")

        handles = handles + [0] * (10 - len(handles))

        return struct.pack(
            formats[self.format_type],
            self.format_type,
            self.port,
            *handles
        )

structs = [
    ChunkServRequest,
    ChunkHB,
    CreateRequest,
    StatusReply,
    ChunkLocRequest,
    ChunkLocReply,
    ChunkRegistration
]

def from_bytes(bytes):
    format_type = struct.unpack("!I", bytes[:4])[0]
    data = struct.unpack(formats[format_type], bytes)


    if format_type == 6:
        return ChunkRegistration(
            format_type=data[0],
            port=data[1],
            chunk_handles=list(data[2:])
        )

    return structs[format_type](*data)
