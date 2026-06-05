import sys
import socket
import struct
import argparse
import math
import shlex

from shared import *

CHUNK_SIZE = 4096 # 4KB
LOCAL_HOST = "127.0.0.1"
master_addr = (LOCAL_HOST, 8888) # Dummy IP address for now
help_text = """CSC569 Final Project Client

Enter commands to perform operations such as read, write, create, delete, etc.

**Commands can also be performed by doing python client.py <command> [command args]

Command List:
    h                                                      Alias for help
    help                                                   Shows this help text.
 
    c                                                      Alias for create
    create <filepath> [chunks]                             Create a new file at filepath.
                                                           If chunks is specified, file will have
                                                           that many chunks (default 1).
 
    d                                                      Alias for delete
    delete <filepath>                                      Delete a file at filepath.

    o                                                      Alias for open
    open <filepath>                                        Open a file at filepath to be ready to operate on

    close <filepath>                                       Close a file at filepath (free cached metadata)

    r                                                      Alias for read
    read [-o OFF] [-s SIZE] <filepath> [dst]               Read a file at filepath
                                                           If dst is specified, ouputs to dst instead of stdout.
                                                           If -o is specified, read from offset
                                                           If -s is specified, read at most s bytes
 
    w                                                      Alias for write
    write <filepath>                                       Write data to a file at filepath.

    s                                                      Alias for snapshot
    snapshot <original> <new>                              Create a copy of the original file/directory
                                                           without copying the data (i.e. copy-on-write)

    a
    append <filepath> <data> <offset>                      Record append operation to append data to a file
                                                           at the specified offset.

    q                                                      Alias for quit
    quit                                                   Exit cleanly
"""

def recvStruct(socket):
	data = socket.recv(4)
	format = formats[struct.unpack("!I", data)[0]]
	struct_size = struct.calcsize(format)
	while len(data) < struct_size:
		recv = socket.recv(struct_size - len(data))
		if not recv:
			raise ConnectionError(f"Connection closed unexpectedly!")
		data += recv
	return from_bytes(data)

def noExitParseArgs(parser, cmd):
	try:
		return parser.parse_args(cmd)
	except argparse.ArgumentError:
		return None

class Client:
	def __init__(self, master_addr, chunk_size=CHUNK_SIZE):
		self.master_addr = master_addr
		self.chunk_size = chunk_size
		self.meta_cache = dict() # Mapping of (filepath, chunkID) to metadata
	
	# Get chunk metadata either from cache or master server
	def getChunkMeta(self, filepath, chunkID):
		if (filepath, chunkID) in self.meta_cache:
			return self.meta_cache[(filepath, chunkID)]

		return self.updateChunkMeta(filepath, chunkID)

	# Force update chunk metadata from master server
	def updateChunkMeta(self, filepath, chunkID):
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
			client_socket.connect(self.master_addr)

			req = ChunkLocRequest(format_type=4, filepath=filepath.encode(), chunk_num=chunkID)
			client_socket.sendall(req.to_bytes())

			reply = recvStruct(client_socket)

			# If reply is a status reply, return it immediately (request failed)
			if reply.format_type == 3:
				self.meta_cache.pop((filepath, chunkID), None)
				return reply
			
			# Otherwise, chunk metadata request was successful
			self.meta_cache[(filepath, chunkID)] = reply
		
		return self.meta_cache[(filepath, chunkID)]

	# Tell the master to create a new file. Returns status code
	def createFile(self, filepath, chunks=1):
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
			client_socket.connect(self.master_addr)

			req = CreateRequest(format_type=2, filepath=filepath.encode(), chunks=chunks)
			client_socket.sendall(req.to_bytes())

			reply = recvStruct(client_socket)
			return reply.status

	# Read chunk from chunkserver
	def readChunk(self, filepath, chunkID):
		# Get chunk metadata and replica address
		chunk_meta = self.getChunkMeta(filepath, chunkID)
		
		# If a StatusReply was received, chunk could not be read, so return b""
		if chunk_meta.format_type == 3:
			return b""

		replica_addr = (LOCAL_HOST, chunk_meta.replica_port)

		# Connect to the replica
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
			status = client_socket.connect_ex(replica_addr)

			# If connection failed, chunk metadata needs to be updated
			while status != 0:
				self.updateChunkMeta(filepath, chunkID)
				new = self.getChunkMeta(filepath, chunkID)

				# If nothing actually updated or StatusReply received, return b"" and print that chunk could not be found
				if new.format_type == 3 or new.chunk_handle == chunk_meta.chunk_handle and new.replica_port == chunk_meta.replica_port:
					print(f"ERROR: Chunk {chunkID} for {filepath} could not be found on replicas!")
					return b""
				
				chunk_meta = new
				replica_addr = (LOCAL_HOST, chunk_meta.replica_port)
				
				# Recreate the socket and try again
				client_socket.close()
				client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				status = client_socket.connect_ex(replica_addr)

			# Perform a read chunk request
			req = ChunkServRequest(format_type=0, req_type=0, chunk_handle=chunk_meta.chunk_handle, data=b"")
			client_socket.sendall(req.to_bytes())

			# Get back reply with the read data
			reply = recvStruct(client_socket)

			# If StatusReply received, something went wrong so update chunk meta and try again
			if reply.format_type == 3:
				self.updateChunkMeta(filepath, chunkID)
				new = self.getChunkMeta(filepath, chunkID)

				# If nothing actually updated or StatusReply received, return b"" and print that chunk could not be found
				if new.format_type == 3 or new.chunk_handle == chunk_meta.chunk_handle and new.replica_port == chunk_meta.replica_port:
					print(f"ERROR: Chunk {chunkID} for {filepath} could not be found on replicas!")
					return b""

				# Otherwise, try again
				return self.readChunk(filepath, chunkID)

			return reply.data

	# Read a file starting from offset. Default is the whole file if size not specified
	def readFile(self, filepath, offset=0, size=-1):
		# Calculate starting chunkID and offset within chunk
		chunkID = offset // self.chunk_size
		chunk_offset = offset % self.chunk_size

		# Read first chunk
		data = self.readChunk(filepath, chunkID)[chunk_offset:]
		if self.chunk_size - chunk_offset > len(data) or size != -1 and size <= self.chunk_size - chunk_offset:
			# If size already satisfied, return data
			return data[:size]
		
		# Otherwise, move on to next chunk
		chunkID += 1
		chunk_data = self.readChunk(filepath, chunkID)
		# TODO: Read until EOF or size satisfied
		while chunk_data != b"" and size != 0:
			data += chunk_data
			if size != -1:
				size -= self.chunk_size

			chunkID += 1
			chunk_data = self.readChunk(filepath, chunkID,)
		
		return data
	
	# Write data to a chunk
	def writeChunk(self, filepath, chunkID, data):
		# Get chunk metadata and replica address
		chunk_meta = self.getChunkMeta(filepath, chunkID)

		# If a StatusReply was received, chunk could not be written to, so return 0 (failed)
		if chunk_meta.format_type == 3:
			return 0

		replica_addr = (LOCAL_HOST, chunk_meta.replica_port)

		# Connect to the replica
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
			status = client_socket.connect_ex(replica_addr)

			# If connection failed, chunk metadata needs to be updated
			while status != 0:
				self.updateChunkMeta(filepath, chunkID)
				new = self.getChunkMeta(filepath, chunkID)

				# If nothing actually updated or StatusReply was received, return 0 and print that chunk could not be found
				if new.format_type == 3 or new.chunk_handle == chunk_meta.chunk_handle and new.replica_port == chunk_meta.replica_port:
					print(f"ERROR: Chunk {chunkID} for {filepath} could not be found on replicas!")
					return 0
				
				chunk_meta = new
				replica_addr = (LOCAL_HOST, chunk_meta.replica_port)

				# Recreate the socket and try again
				client_socket.close()
				client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				status = client_socket.connect_ex(replica_addr)

			# Perform a write chunk request
			req = ChunkServRequest(format_type=0, req_type=1, chunk_handle=chunk_meta.chunk_handle, data=data)
			client_socket.sendall(req.to_bytes())

			# Return the reply status code
			reply = recvStruct(client_socket)

			# If StatusReply received, could be outdated meta, so update chunk meta and try again
			if reply.format_type == 3:
				self.updateChunkMeta(filepath, chunkID)
				new = self.getChunkMeta(filepath, chunkID)

				# If nothing actually updated or StatusReply received, return reply.status. Something else went wrong
				if new.format_type == 3 or new.chunk_handle == chunk_meta.chunk_handle and new.replica_port == chunk_meta.replica_port:
					return reply.status

				# Otherwise, try again
				return self.writeChunk(filepath, chunkID, data)
			
			return reply.status

	# Write everything to the file
	def writeFile(self, filepath, data):
		# Loop for each chunk being edited
		for chunkID in range(math.ceil(len(data) / self.chunk_size)):
			chunk_data = data[chunkID * self.chunk_size: (chunkID + 1) * self.chunk_size]
			status = self.writeChunk(filepath, chunkID, chunk_data)

			# If something went wrong, return the error code and the chunk
			if status != 1:
				return (status, chunkID)
		
		# Otherwise, (1, 0) indicates success
		return (1, 0)

	# Parse and perform a command. Returns true if exit
	def performCmd(self, cmd):
		if len(cmd) < 1:
			print("Invalid operation! Type \"help\" or \"h\" for a list of valid operations.")
			return

		op = cmd[0]

		match op:
			case "help" | "h":
				print(help_text)
			case "quit" | "q":
				print("Exited")
				return True
			case "create" | "c":
				# If not enough arguments, return
				if len(cmd) < 2:
					print("Usage: create <filepath> [chunks]")
					return

				# Chunks is 1 by default
				chunks = 1
				try:
					# If specified, use specified chunks
					if len(cmd) > 2:
						chunks = int(cmd[2])
				except:
					pass
					print("WARNING: Invalid chunk count! Defaulting to 1.")
				
				status = self.createFile(cmd[1], chunks)

				if status != 1:
					print(f"ERROR: File creation failed. File may have already been created.")
				else:
					print(f"{cmd[1]} was successfully created with {chunks} chunks")
			case "delete" | "d":
				print("Delete in progress lmao")
			case "open" | "o":
				print("Open in progress lmao")
			case "close":
				print("Close in progress lmao")
			case "read" | "r":
				# Argument parser specifically for read
				parser = argparse.ArgumentParser(exit_on_error=False)
				parser.add_argument('filename')
				parser.add_argument('dst', nargs="?")
				parser.add_argument('-o', type=int)
				parser.add_argument('-s', type=int)

				# Parse the command
				args = noExitParseArgs(parser, cmd[1:])

				# If invalid parse, don't proceed
				if args is None:
					print("Usage: read [-o OFF] [-s SIZE] <filepath> [dst]")
					return

				off = args.o or 0
				size = args.s if args.s is not None else -1
				
				# Read file
				data = self.readFile(args.filename, off, size)
				
				# OLD: Read by chunk. Comment readFile and uncomment this if readFile is breaking stuff
				# data = self.readChunk(args.filename, args.chunk_num)
				# data = data[off:off + size]

				# Turn data to actual text
				text = data.decode().rstrip('\x00')

				# Output to file if specified
				if args.dst:
					with open(args.dst, "w") as f:
						f.write(text)
				else:
					print(text)
			case "write" | "w":
				if len(cmd) < 2:
					print("Usage: write <filepath>")
					return

				print("Write file contents here. EOF (CTRL+D) to end.")

				# Read input
				input_str = sys.stdin.read()

				# Print for a new line after input
				print()

				status, chunkID = self.writeFile(cmd[1], input_str.encode())
				if status != 1:
					print(f"ERROR: Chunk {chunkID} could not be written to. File may not be big enough or might not exist at all.")
				else:
					print("Write successful")
			case "snapshot" | "s":
				print("Snapshot in progress lmao")
			case "append" | "a":
				print("Record append in progress lmao")
			case _:
				print("Invalid operation! Type \"help\" or \"h\" for a list of valid operations.")

def main():
	client = Client(master_addr, CHUNK_SIZE)

	if len(sys.argv) > 1:
		client.performCmd(sys.argv[1:])
		return

	print("CSC569 Final Project Client is now running...\n")

	# Main loop
	while True :
		cmd = None
		# Prompt for operation
		try:
			# Using shlex.split to mimic actual shell argv behavior
			cmd = shlex.split(input("Command:"))
		except EOFError:
			print("\nEOF encountered. Terminating client session...")
			return

		if client.performCmd(cmd):
			# If return true, signal to exit
			break

if __name__ == "__main__":
	main()