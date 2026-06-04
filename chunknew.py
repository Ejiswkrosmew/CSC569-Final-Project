from mpi4py import MPI
import socket
import struct
import threading
from shared import ChunkHB, ChunkRegistration, ChunkServRequest, StatusReply, from_bytes, formats
import time

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 8888
chunk_store = {}
op_log = []
log_lock = threading.Lock()
master_lock = threading.Lock()

def log_op(chunk_handle, op_type):
    with log_lock:
        op_log.append((chunk_handle, op_type))


def recv_exact(sock, size: int) -> bytes:
    data = b""

    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("socket closed before full message received")
        data += chunk

    return data


def recv_decode(sock):
    header = recv_exact(sock, 4)
    format_type = struct.unpack("!I", header)[0]
    total_size = struct.calcsize(formats[format_type])
    rest = recv_exact(soct, total_size - 4)
    return from_bytes(header + rest)


def handle_chunkserver_client(conn, chunks: dict[int, bytes], comm):
    try:
        request_size = struct.calcsize(formats[0])
        raw = recv_exact(conn, request_size)

        req = from_bytes(raw)

        if not isinstance(req, ChunkServRequest):
            raise ValueError("expected ChunkServRequest")

        if req.req_type == 0: #read
            if req.chunk_handle not in chunks:
                    raise ValueError(f"chunk {req.chunk_handle} not found")
            
            print(f"recieved read request, sending chunk {req.chunk_handle}")


            log_op(req.chunk_handle, 0)

            reply = ChunkServRequest(
                format_type=0,
                req_type=0,
                chunk_handle=req.chunk_handle,
                data = chunks[req.chunk_handle]
            )

        elif req.req_type == 1: #write
            chunks[req.chunk_handle] = req.data

            print(f"recieved write request, sending request to replicas")

            log_op(req.chunk_handle, 1)
            
            for replica_rank in range(1, comm.Get_size()):
                comm.send(
                    {
                        "op": "write_chunk",
                        "chunk_handle": req.chunk_handle,
                        "data": req.data,
                    },
                    dest=replica_rank,
                    tag=100
                )

            # Wait for every replica to acknowledge
            for replica_rank in range(1, comm.Get_size()):
                ack = comm.recv(source=replica_rank, tag=101)

                if ack.get("status") != 1:
                    raise ValueError(f"replica {replica_rank} failed to store chunk")
 
            print("replicas wrote chunk, sending status reply")

            reply = StatusReply(
                format_type=3,
                status=1
            )


    except Exception as e:
        print(f"chunkserver handler error: {e}")

        reply = StatusReply(
            format_type=3,
            status=0
        )

    conn.sendall(reply.to_bytes())
    conn.close()

def hb_loop(master_sock, port):
    while(2):
        time.sleep(3) #adjust for frequency

        if op_log:
            log_present = True
            handle, op_type = op_log.pop(0)
        else:
            log_present = False
            handle, op_type = 0, 3 #idk what to make it but we ignore it so whatever

        hb =  ChunkHB(
            format_type = 1,
            src_port = port,
            log_entry = log_present,
            chunk_handle = handle,
            op_type = op_type
        )
        
        with master_lock:
            master_sock.sendall(hb.to_bytes())

def listen_for_master_chunkreq(master_sock, chunks):
    while True:
        try:
            req = recv_decode(master_sock)

            if isinstance(req, ChunkServRequest):
                if req.req_type == 0: #read
                    if req.chunk_handle not in chunks:
                        raise ValueError(f"chunk {req.chunk_handle} not found")

                    print(f"recieved read request from master, sending chunk {req.chunk_handle}")

                    reply = ChunkServRequest(
                        format_type=0,
                        req_type=0,
                        chunk_handle=req.chunk_handle,
                        data = chunks[req.chunk_handle]
                    )

                elif req.req_type == 1: #write
                    chunks[req.chunk_handle] = req.data

                    print(f"recieved write request from master, sending request to replicas")

                    for replica_rank in range(1, comm.Get_size()):
                        comm.send(
                            {
                                "op": "write_chunk",
                                "chunk_handle": req.chunk_handle,
                                "data": req.data,
                            },
                            dest=replica_rank,
                            tag=100
                        )

                    # Wait for every replica to acknowledge
                    for replica_rank in range(1, comm.Get_size()):
                        ack = comm.recv(source=replica_rank, tag=101)

                        if ack.get("status") != 1:
                            raise ValueError(f"replica {replica_rank} failed to store chunk")
 
                    print("replicas wrote chunk from master, sending status reply")

                    reply = StatusReply(
                        format_type=3,
                        status=1
                    )
                with master_lock:
                    master_sock.sendall(reply.to_bytes())
            else:
                raise ValueError(f"recieved unexpected struct from master")
        except Exception as e:
            print(f"connection with master was broken: {e}")
            break


def replicaHB(comm):
    rank = comm.Get_rank()
    size = comm.Get_size()
    while True:
        time.sleep(1)
        print("rank0 sending hbs")
        for replica in range(1, size):
            comm.send(
                    {
                        "hb": "true"
                    },
                    dest=replica,
                    tag=200
                    )

def replica_list_hb(comm):
    rank = comm.Get_rank()
    while True:

        timeout = 5.0 #primary crash detection timeout
        start = time.time()

        req = comm.irecv(source=0, tag=200)

        while True:
            msg = req.test()

            if msg[0]:
                print(f"heartbeat from 0 {msg}")
                break
            
            if time.time() - start > timeout:
                print("rank zero has gone down")
                #TODO:
                #write entire chunkstore to file <chunkdump-<portnumber>>
                #exit
                break


            time.sleep(0.01)


def main(): #TODO: arg order: chunkserverport#, masterport#, chunkdumpfilepath  
    comm = MPI.COMM_WORLD
    #print(f"Hello from rank {comm.Get_rank()} of {comm.Get_size()}")
    rank = comm.Get_rank()
    size = comm.Get_size()
    HOST = "0.0.0.0"
    PORT = 5000

    if rank == 0:
        print(f"I am master")
        
        #TODO:
        #read from chunkdump file on disk (./chunkdump-<portnumber>)
        #place into chunkstore
        #destory file

        #connect to master
        master_sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        master_sock.connect(
            (MASTER_HOST, MASTER_PORT)
        )

        registration = ChunkRegistration(
            format_type = 6,
            port = PORT,
            chunk_handles = list(chunk_store.keys())
        )

        with master_lock:
            master_sock.sendall(
                registration.to_bytes()
            )

        threading.Thread(
            #heartbeat loop
            target = hb_loop,
            args=(master_sock, PORT),
            daemon=True
        ).start()


        threading.Thread(
                #listen for requests from master
                target=listen_for_master_chunkreq,
                args=(master_sock, chunk_store),
                daemon=True
        ).start()


        threading.Thread(
            #send HB to replicas
            target=replicaHB,
            args=(comm,),
            daemon=True
        ).start()



        #listen for connections from the client and handle
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serversocket:
            serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            serversocket.bind((HOST, PORT))
            serversocket.listen()

            print(f"Rank 0 listening on {HOST}:{PORT}")


            exit
            while True:
                clientsocket, address = serversocket.accept()

                client_thread = threading.Thread(
                    target=handle_chunkserver_client,
                    args=(clientsocket, chunk_store, comm),
                    daemon=True
                )
                client_thread.start()

    elif rank in [1, 2, 3]:
        print(f"I am replica rank {rank}")

        threading.Thread(
            target=replica_list_hb,
            args=(comm,),
            daemon=True
        ).start()


        while True:
            msg = comm.recv(source=0, tag=100)

            try:
                if msg["op"] == "write_chunk":
                    chunk_handle = msg["chunk_handle"]
                    data = msg["data"]

                    print(f"rank {rank} process writing chunk {chunk_handle}")

                    chunk_store[chunk_handle] = data

                    comm.send(
                        {
                            "status": 1,
                            "chunk_handle": chunk_handle,
                        },
                        dest=0,
                        tag=101
                    )

            except Exception as e:
                print(f"Replica {rank} error: {e}")

                comm.send(
                    {
                        "status": 0,
                        "error": str(e),
                    },
                    dest=0,
                    tag=101
                )



if __name__ == "__main__":
    main()
