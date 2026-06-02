import struct
from dataclasses import dataclass

formats = {
    0: "!III4096s",
    1: "!I?II",
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
    type: int
    chunk_handle: int
    data: bytes[4096]

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.type, self.chunk_handle, self.data)

# HB for chunkservers to send to the master
@dataclass
class ChunkHB:
    format_type: int = 1
    log_entry: bool
    chunk_handle: int
    op_type: int

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.log_entry, self.chunk_handle, self.op_type)

# Request to create a new file from the client to the master
@dataclass
class CreateRequest:
    format_type: int = 2
    filepath: bytes[50]
    chunks: int

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.filepath, self.chunks)

# Generic status reply struct
@dataclass
class StatusReply:
    format_type: int = 3
    status: int

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.status)

# Client request the location of a chunk from the master
@dataclass
class ChunkLocRequest:
    format_type: int = 4
    filepath: bytes[50]
    chunk_num: int

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.filepath, self.chunk_num)

# Master’s reply of where a chunk is located at
@dataclass
class ChunkLocReply:
    format_type: int = 5
    chunk_handle: int
    replica_port: int

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.chunk_handle, self.replica_port)

# Initial chunkserver telling master about itself
@dataclass
class ChunkRegistration:
    format_type: int = 6
    port: int
    chunk_handles: list[int]

    def to_bytes(self):
        return struct.pack(formats[self.format_type], self.format_type, self.port, *self.chunk_handles)

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
    return structs[format_type](*data)