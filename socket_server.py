import json
import socket
import threading
import time
from typing import Any

HOST = "0.0.0.0"
PORT = 9000

ENCODING = "utf-8"
DELIMITER = b"\n"
RECEIVE_CHUNK_SIZE = 4096
MAX_RECEIVE_BUFFER_SIZE = 256 * 1024


# =========================================================
# FIXED MESSAGE TYPES
# =========================================================

# Roku client -> Python server
CLIENT_MESSAGE_TYPES = {
    "CLIENT_HELLO",
    "CLIENT_PING",
    "CLIENT_PONG",
    "CLIENT_JOIN_ROOM",
    "CLIENT_LEAVE_ROOM",
    "CLIENT_PLAYER_INPUT",
}

# Python server -> Roku client
SERVER_MESSAGE_TYPES = {
    "SERVER_WELCOME",
    "SERVER_PING",
    "SERVER_PONG",
    "SERVER_ROOM_JOINED",
    "SERVER_ROOM_LEFT",
    "SERVER_GAME_STATE",
    "SERVER_ERROR",
}


# Used only to display cleaner thread-safe console output.
print_lock = threading.Lock()


def log(*values: Any) -> None:
    with print_lock:
        print(*values, flush=True)


# =========================================================
# CLIENT CONNECTION STATE
# =========================================================

class ClientConnection:
    def __init__(
        self,
        client_socket: socket.socket,
        client_address: tuple[str, int],
    ) -> None:
        self.socket = client_socket
        self.address = client_address

        self.receive_buffer = bytearray()
        self.send_sequence = 0

        self.player_id: str | None = None
        self.room_id: str | None = None

        self.connected = True
        self.send_lock = threading.Lock()

    def next_sequence(self) -> int:
        self.send_sequence += 1
        return self.send_sequence

    def close(self) -> None:
        self.connected = False

        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        try:
            self.socket.close()
        except OSError:
            pass


# =========================================================
# SEND JSON MESSAGE
# =========================================================

def send_message(
    client: ClientConnection,
    message_type: str,
    data: dict[str, Any] | None = None,
) -> bool:
    """
    Sends one newline-delimited JSON message.

    Example:
    {"type":"SERVER_WELCOME","seq":1,"ts":123,"data":{}}\n
    """

    if message_type not in SERVER_MESSAGE_TYPES:
        log(
            f"[SERVER ERROR] Attempted to send unknown type: "
            f"{message_type}"
        )
        return False

    if data is None:
        data = {}

    message = {
        "type": message_type,
        "seq": client.next_sequence(),
        "ts": int(time.time()),
        "data": data,
    }

    try:
        json_string = json.dumps(
            message,
            separators=(",", ":"),
            ensure_ascii=False,
        )

        packet = json_string.encode(ENCODING) + DELIMITER

        # More than one server thread may send to a client,
        # such as a delayed-message thread.
        with client.send_lock:
            client.socket.sendall(packet)

        log(
            f"[SENT] {client.address} "
            f"type={message_type} "
            f"bytes={len(packet)}"
        )
        log(f"[SENT JSON] {json_string}")

        return True

    except BrokenPipeError:
        log(f"[SEND ERROR] Broken pipe: {client.address}")

    except ConnectionResetError:
        log(f"[SEND ERROR] Connection reset: {client.address}")

    except OSError as error:
        log(f"[SEND ERROR] {client.address}: {error}")

    client.connected = False
    return False


# =========================================================
# SEND ERROR MESSAGE
# =========================================================

def send_error(
    client: ClientConnection,
    error_code: str,
    error_message: str,
    request_sequence: Any = None,
) -> bool:
    data: dict[str, Any] = {
        "code": error_code,
        "message": error_message,
    }

    if request_sequence is not None:
        data["requestSeq"] = request_sequence

    return send_message(
        client,
        "SERVER_ERROR",
        data,
    )


# =========================================================
# PROCESS CLIENT MESSAGE
# =========================================================

def process_client_message(
    client: ClientConnection,
    message: dict[str, Any],
) -> None:
    message_type = message.get("type")
    request_sequence = message.get("seq")
    data = message.get("data")

    if not isinstance(message_type, str):
        send_error(
            client,
            "MISSING_MESSAGE_TYPE",
            "Incoming message does not contain a valid type.",
            request_sequence,
        )
        return

    if message_type not in CLIENT_MESSAGE_TYPES:
        send_error(
            client,
            "UNKNOWN_MESSAGE_TYPE",
            f"Unknown client message type: {message_type}",
            request_sequence,
        )
        return

    if data is None:
        data = {}

    if not isinstance(data, dict):
        send_error(
            client,
            "INVALID_MESSAGE_DATA",
            "Message data must be a JSON object.",
            request_sequence,
        )
        return

    log(
        f"[MESSAGE] {client.address} "
        f"type={message_type} "
        f"seq={request_sequence}"
    )
    log(f"[MESSAGE DATA] {data}")

    # -----------------------------------------------------
    # CLIENT_HELLO
    # -----------------------------------------------------
    if message_type == "CLIENT_HELLO":
        player_id = data.get("playerId")
        app_version = data.get("appVersion")
        platform = data.get("platform")

        if isinstance(player_id, str):
            client.player_id = player_id

        log(
            f"[CLIENT HELLO] playerId={player_id}, "
            f"appVersion={app_version}, "
            f"platform={platform}"
        )

        send_message(
            client,
            "SERVER_WELCOME",
            {
                "message": "Connected to Python socket server",
                "playerId": player_id,
                "serverTime": int(time.time()),
                "requestSeq": request_sequence,
            },
        )

    # -----------------------------------------------------
    # CLIENT_PING
    # -----------------------------------------------------
    elif message_type == "CLIENT_PING":
        send_message(
            client,
            "SERVER_PONG",
            {
                "requestSeq": request_sequence,
                "clientTime": data.get("clientTime"),
                "serverTime": int(time.time()),
            },
        )

    # -----------------------------------------------------
    # CLIENT_PONG
    # -----------------------------------------------------
    elif message_type == "CLIENT_PONG":
        log(
            f"[CLIENT PONG] {client.address} "
            f"serverSeq={data.get('serverSeq')}"
        )

    # -----------------------------------------------------
    # CLIENT_JOIN_ROOM
    # -----------------------------------------------------
    elif message_type == "CLIENT_JOIN_ROOM":
        room_id = data.get("roomId")

        if not isinstance(room_id, str) or not room_id.strip():
            send_error(
                client,
                "INVALID_ROOM_ID",
                "roomId must be a non-empty string.",
                request_sequence,
            )
            return

        client.room_id = room_id

        log(
            f"[ROOM JOINED] client={client.address}, "
            f"roomId={room_id}"
        )

        send_message(
            client,
            "SERVER_ROOM_JOINED",
            {
                "roomId": room_id,
                "playerId": client.player_id,
                "requestSeq": request_sequence,
            },
        )

    # -----------------------------------------------------
    # CLIENT_LEAVE_ROOM
    # -----------------------------------------------------
    elif message_type == "CLIENT_LEAVE_ROOM":
        requested_room_id = data.get("roomId")
        previous_room_id = client.room_id

        client.room_id = None

        log(
            f"[ROOM LEFT] client={client.address}, "
            f"roomId={previous_room_id}"
        )

        send_message(
            client,
            "SERVER_ROOM_LEFT",
            {
                "roomId": (
                    previous_room_id
                    if previous_room_id is not None
                    else requested_room_id
                ),
                "requestSeq": request_sequence,
            },
        )

    # -----------------------------------------------------
    # CLIENT_PLAYER_INPUT
    # -----------------------------------------------------
    elif message_type == "CLIENT_PLAYER_INPUT":
        action = data.get("action")
        pressed = data.get("pressed", True)

        if not isinstance(action, str) or not action.strip():
            send_error(
                client,
                "INVALID_PLAYER_ACTION",
                "Player input action must be a non-empty string.",
                request_sequence,
            )
            return

        log(
            f"[PLAYER INPUT] playerId={client.player_id}, "
            f"roomId={client.room_id}, "
            f"action={action}, "
            f"pressed={pressed}"
        )

        # Test response. Replace this data with your actual
        # authoritative game state.
        send_message(
            client,
            "SERVER_GAME_STATE",
            {
                "event": "PLAYER_INPUT_RECEIVED",
                "playerId": client.player_id,
                "roomId": client.room_id,
                "action": action,
                "pressed": pressed,
                "requestSeq": request_sequence,
            },
        )

        # Optional delayed JSON message for async testing.
        threading.Thread(
            target=send_delayed_game_state,
            args=(client, request_sequence),
            daemon=True,
        ).start()


# =========================================================
# OPTIONAL DELAYED ASYNC MESSAGE
# =========================================================

def send_delayed_game_state(
    client: ClientConnection,
    request_sequence: Any,
) -> None:
    time.sleep(1)

    if not client.connected:
        return

    send_message(
        client,
        "SERVER_GAME_STATE",
        {
            "event": "DELAYED_TEST_MESSAGE",
            "message": "Message sent one second later",
            "requestSeq": request_sequence,
        },
    )


# =========================================================
# PROCESS ONE COMPLETE JSON LINE
# =========================================================

def process_json_line(
    client: ClientConnection,
    raw_line: bytes,
) -> None:
    # Support both "\n" and "\r\n".
    raw_line = raw_line.rstrip(b"\r")

    if not raw_line:
        return

    log(f"[RAW MESSAGE BYTES] {raw_line!r}")
    log(f"[RAW MESSAGE LENGTH] {len(raw_line)}")

    try:
        decoded_line = raw_line.decode(ENCODING)
    except UnicodeDecodeError as error:
        log(f"[UTF-8 ERROR] {client.address}: {error}")

        send_error(
            client,
            "INVALID_UTF8",
            "Message must be valid UTF-8.",
        )
        return

    log(f"[DECODED MESSAGE] {decoded_line}")

    try:
        message = json.loads(decoded_line)
    except json.JSONDecodeError as error:
        log(
            f"[JSON ERROR] {client.address}: "
            f"{error.msg} at position {error.pos}"
        )

        send_error(
            client,
            "INVALID_JSON",
            f"Could not parse JSON: {error.msg}",
        )
        return

    if not isinstance(message, dict):
        send_error(
            client,
            "INVALID_MESSAGE",
            "Socket message must be a JSON object.",
        )
        return

    process_client_message(client, message)


# =========================================================
# PROCESS RECEIVE BUFFER
# =========================================================

def process_receive_buffer(
    client: ClientConnection,
) -> None:
    """
    Handles all TCP packet combinations:

    1. One JSON message in one recv()
    2. One JSON message split across several recv() calls
    3. Multiple JSON messages in one recv()
    4. Complete messages plus part of the next message
    """

    while True:
        newline_index = client.receive_buffer.find(DELIMITER)

        if newline_index < 0:
            break

        raw_line = bytes(
            client.receive_buffer[:newline_index]
        )

        del client.receive_buffer[:newline_index + 1]

        process_json_line(client, raw_line)


# =========================================================
# HANDLE ONE CONNECTED CLIENT
# =========================================================

def handle_client(
    client_socket: socket.socket,
    client_address: tuple[str, int],
) -> None:
    client = ClientConnection(
        client_socket,
        client_address,
    )

    log("=" * 70)
    log(f"[CONNECTED] {client_address}")

    try:
        # Blocking socket is fine because every connected
        # client has its own thread.
        client_socket.settimeout(None)

        # Optional immediate connection message.
        # The Roku manager accepts SERVER_WELCOME.
        send_message(
            client,
            "SERVER_WELCOME",
            {
                "message": "TCP connection established",
                "serverTime": int(time.time()),
            },
        )

        while client.connected:
            log(f"[WAITING FOR DATA] from {client_address}")

            received_data = client_socket.recv(
                RECEIVE_CHUNK_SIZE
            )

            if not received_data:
                log(
                    f"[DISCONNECTED] Client closed connection: "
                    f"{client_address}"
                )
                break

            log(f"[RECEIVED TCP CHUNK] {received_data!r}")
            log(
                f"[RECEIVED TCP CHUNK LENGTH] "
                f"{len(received_data)}"
            )

            client.receive_buffer.extend(received_data)

            if (
                len(client.receive_buffer)
                > MAX_RECEIVE_BUFFER_SIZE
            ):
                send_error(
                    client,
                    "RECEIVE_BUFFER_OVERFLOW",
                    "Receive buffer exceeded maximum size.",
                )

                log(
                    f"[BUFFER OVERFLOW] Closing "
                    f"{client_address}"
                )
                break

            process_receive_buffer(client)

    except ConnectionResetError:
        log(
            f"[ERROR] Connection reset by client: "
            f"{client_address}"
        )

    except BrokenPipeError:
        log(
            f"[ERROR] Broken pipe: "
            f"{client_address}"
        )

    except OSError as error:
        log(
            f"[SOCKET ERROR] {client_address}: "
            f"{error}"
        )

    except Exception as error:
        log(
            f"[UNEXPECTED ERROR] {client_address}: "
            f"{type(error).__name__}: {error}"
        )

    finally:
        client.close()

        log(f"[CLOSED] {client_address}")
        log("=" * 70)


# =========================================================
# START TCP SERVER
# =========================================================

def start_server() -> None:
    server_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM,
    )

    server_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1,
    )

    server_socket.bind((HOST, PORT))
    server_socket.listen(10)

    log(f"[SERVER STARTED] {HOST}:{PORT}")
    log("[PROTOCOL] Newline-delimited JSON")
    log("[WAITING FOR CLIENTS]")

    try:
        while True:
            client_socket, client_address = (
                server_socket.accept()
            )

            client_thread = threading.Thread(
                target=handle_client,
                args=(
                    client_socket,
                    client_address,
                ),
                daemon=True,
            )

            client_thread.start()

    except KeyboardInterrupt:
        log("\n[SERVER STOPPING]")

    finally:
        server_socket.close()
        log("[SERVER CLOSED]")


if __name__ == "__main__":
    start_server()