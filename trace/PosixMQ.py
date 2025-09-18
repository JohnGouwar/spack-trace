from ctypes import CDLL, POINTER, c_char_p, c_int, c_size_t, c_uint, byref, create_string_buffer
from pathlib import Path


FFI = CDLL(f"{Path(__file__).parent / 'posixmq.so'}")

posixmq_open_create = FFI.posixmq_open_create
posixmq_open_create.argtypes = [c_char_p, c_size_t, c_size_t, POINTER(c_int)]
posixmq_open_existing = FFI.posixmq_open_existing
posixmq_open_existing.argtypes = [c_char_p, POINTER(c_int), POINTER(c_size_t), POINTER(c_size_t)]
posixmq_close = FFI.posixmq_close
posixmq_close.argtypes = [c_int]
posixmq_unlink = FFI.posixmq_unlink
posixmq_unlink.argtypes = [c_char_p]
posixmq_send = FFI.posixmq_send
posixmq_send.argtypes = [c_int, c_char_p, c_size_t, c_uint]
posixmq_recv = FFI.posixmq_recv
posixmq_recv.argtypes = [c_int, c_char_p, POINTER(c_size_t), POINTER(c_uint)]

class PosixMQ:
    def __init__(
            self,
            name: str,
            queue_fd: int,
            max_queue_size,
            max_msg_size,
    ):
        self.name = name
        self.fd = queue_fd
        self.max_queue_size = max_queue_size
        self.max_msg_size = max_msg_size
        
    @classmethod
    def create(
            cls,
            name: str,
            max_queue_size: int = 10,
            max_msg_size = 4096
    ) -> "PosixMQ":
        queue_id = c_int()
        res = posixmq_open_create(
            name.encode(),
            max_msg_size,
            max_queue_size,
            byref(queue_id)
        )
        if res != 0:
            print("create failed")
        return cls(name, queue_id.value, max_queue_size, max_msg_size)

    
    @classmethod
    def open(cls, name: str) -> "PosixMQ":
        queue_id = c_int()
        max_queue_size = c_size_t()
        max_msg_size = c_size_t()
        res = posixmq_open_existing(
            name.encode(),
            byref(queue_id),
            byref(max_msg_size),
            byref(max_queue_size),
        )
        if res != 0:
            print("open failed")
        return cls(name, queue_id.value, max_queue_size.value, max_msg_size.value) 
    
    def close(self):
        posixmq_close(self.fd)

    def unlink(self):
        posixmq_unlink(self.name.encode())
        
    def send(self, data: str, prio: int):
        data_bytes = data.encode()
        res = posixmq_send(self.fd, data_bytes, len(data_bytes), prio)
        if res != 0:
            print("send failed")
    def recv(self) -> str:
        buf_out = create_string_buffer(self.max_msg_size)
        size_inout = c_size_t(self.max_msg_size)
        priority_out = c_uint()
        res = posixmq_recv(
            self.fd,
            buf_out,
            size_inout,
            priority_out
        )
        if res != 0:
            print("recv failed")
        return buf_out.value[:size_inout.value].decode()
