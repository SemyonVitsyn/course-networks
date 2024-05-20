import socket
import struct
import threading


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def close(self):
        self.udp_socket.close()


def encode(seq, sent_blocks, data):
    return struct.pack('!QQ', seq, sent_blocks) + data


def decode(block):
    return struct.unpack(f'!QQ{len(block)-16}s', block)


class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.block_size = 60000
        self.seq = 0
        self.ack = 0

        self.threads = []
        self.thread_count = 0
        self.recv_threads = 0

        self.sent_blocks = 0
        self.recv_blocks = 0

    def send(self, data: bytes):
        thread = threading.Thread(target=self.sendData, args=(data, self.thread_count))
        self.thread_count += 1
        self.threads.append(thread)
        thread.start()

        return len(data)

    def sendData(self, data: bytes, thread_number):
        if thread_number != 0:
            self.threads[thread_number-1].join()

        processed = 0
        length = len(data)
        while processed < length:
            diff = min(self.block_size, length - processed)
            cur_data = data[processed:processed+diff]
            self.seq += diff
            processed += diff 

            self.sent_blocks += 1
            block = encode(self.seq, self.sent_blocks, cur_data)
            iter = 0

            while True:
                self.sendto(block)
                iter += 1
                if self.thread_count - thread_number <= 1 and iter >= 60:
                    break
                self.udp_socket.settimeout(0.001)
                if processed == length:
                    try:
                        recv = self.recvfrom(16)
                        recv_count, recv_thread_number = struct.unpack('!QQ', recv)
                        if (recv_count == self.sent_blocks and recv_thread_number == thread_number + 1):
                            break

                    except socket.timeout:
                        continue

    def recv(self, n: int):
        processed = 0
        block_number = 0
        blocks_count = n // self.block_size + 1
        recv_data = b''

        while block_number != blocks_count:
            try:
                self.udp_socket.settimeout(0.001)
                block = self.recvfrom(min(self.block_size, n - processed) + 16)
                if (len(block) <= 16):
                    continue

                seq, sent_blocks, data = decode(block)

                if seq == self.ack + len(data) and sent_blocks == self.recv_blocks + 1:
                    recv_data += data
                    self.ack += len(data)
                    processed += len(data)

                    block_number += 1
                    self.recv_blocks += 1
                else:
                    block = struct.pack('!QQ', self.recv_blocks, self.recv_threads)
                    self.sendto(block)
            except socket.timeout:
                block = struct.pack('!QQ', self.recv_blocks, self.recv_threads)
                self.sendto(block)
                continue

        self.recv_threads += 1
        block = struct.pack('!QQ', self.recv_blocks, self.recv_threads)
        for _ in range(5):
            self.sendto(block)
        return recv_data

    def close(self):
        self.threads[-1].join()
        super().close()