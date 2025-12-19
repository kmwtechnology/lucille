import sys
from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters
import time
import json
import os
import threading
import base64
from py4j.protocol import Py4JNetworkError

PROCESS_FUNCTION_NAME = "process_document"

class TeeLogger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "a", buffering=1)
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        self.terminal.flush()
        self.log.flush()

class Py4jClient:
    def __init__(self, script_code: str, port: int = 25333):
        # IMPORTANT: do not print the entire script code
        print(f"[Py4jClient] Initializing Py4jClient with script_code length: {len(script_code)} and port: {port}")
        self.script_code = script_code
        self.port = port
        self.gateway = None
        self.running = False

    def exec(self, json_msg):
        #print(f"[Py4jClient] exec called with json_msg: {json_msg}")
        msg = json.loads(json_msg)
        #print(f"[Py4jClient] Executing msg: {msg}")

        func = globals().get(PROCESS_FUNCTION_NAME)
        if func is None or not callable(func):
          raise AttributeError(f"Required method '{PROCESS_FUNCTION_NAME}' not found or not callable")

        data = msg.get("data")
        if data is None or data == []:
          result = func()
        else:
          result = func(data)

        if result is None:
          return None

        if isinstance(result, str):
          return result
        else:
          try:
            return json.dumps(result)
          except TypeError as e:
            print(f"[Py4jClient] Result from {PROCESS_FUNCTION_NAME} is not JSON-serializable: {e}")
            raise

    def start(self):
        # Set up tee logging to both stdout and a file
        log_file = os.path.abspath("python/py4jclient.log")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        sys.stdout = TeeLogger(log_file)
        sys.stderr = TeeLogger(log_file)

        print(f"[Py4jClient] Logging to stdout and {log_file}")
        print(f"[Py4jClient] Loading user module from script code (length={len(self.script_code)})")

        # Load the user script into the global namespace so its functions are available in globals()
        exec(self.script_code, globals())

        try:
            self.gateway = JavaGateway(
                gateway_parameters=GatewayParameters(auto_convert=True, port=self.port, auto_close=True, read_timeout=5),
                callback_server_parameters=CallbackServerParameters(port=self.port + 1, daemonize_connections=True),
                python_server_entry_point=self,
            )
            # Java StreamConsumer looks for the string "JavaGateway STARTED" in python output.
            print(
                "[Py4jClient] JavaGateway STARTED. Callback server port: ",
                self.gateway.get_callback_server().get_listening_port(),
            )
        except Exception as e:
            print("[Py4jClient] Error connecting to JavaGateway:", e)
            self.running = False
            sys.exit(1)

        self.running = True
        self._monitor_thread = threading.Thread(target=self.monitor_connection, daemon=True)
        self._monitor_thread.start()

    def stop(self):
        print("[Py4jClient] Stopping client.")
        self.running = False
        if self.gateway is not None:
            self.gateway.shutdown()
            print("[Py4jClient] Gateway shutdown.")

    def monitor_connection(self):
        """Background thread: exits process if Java side is gone."""
        while self.running:
            try:
                self.gateway.jvm.System.currentTimeMillis()
            except (Py4JNetworkError, EOFError, OSError) as e:
                print(f"[Py4jClient] Lost connection to Java: {e}, exiting.")
                self.stop()
                os._exit(1)
            time.sleep(2)


def _decode_script_b64(b64: str) -> str:
    if b64 is None or b64.strip() == "":
        raise ValueError("Missing required argument: --script-code-b64")

    try:
        raw = base64.b64decode(b64.encode("ascii"), validate=True)
        return raw.decode("utf-8")
    except Exception as e:
        raise ValueError(f"Failed to decode --script-code-b64 as base64 UTF-8: {e}") from e


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Py4jClient for Lucille PythonStage integration")
    parser.add_argument("--script-code-b64", required=True, help='Base64-encoded UTF-8 python script contents')
    parser.add_argument("--port", required=True, type=int, help='Port for Py4J Gateway')
    args = parser.parse_args()

    script_code = _decode_script_b64(args.script_code_b64)

    client = Py4jClient(script_code, args.port)
    client.start()
    print(f"[Py4jClient] Started client with script code length: {len(script_code)} and port: {args.port}")
