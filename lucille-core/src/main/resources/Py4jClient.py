import sys
from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters
import importlib.util
import time
import json

class Py4jClient:
    def __init__(self, script_path, port=25333):
        self.script_path = script_path
        self.port = port
        self.gateway = None
        self.running = False
        self.user_module = None

    def exec(self, code):
        print(f"[Py4jClient] Executing code: {code}")
        try:
            exec(code)
        except Exception as e:
            print(f"[Py4jClient] Error executing code: {e}")

    def send(self, json_msg):
        msg = json.loads(json_msg)
        print(f"[Py4jClient] Executing msg: {msg}")
        if msg.get("data") is None or msg.get("data") == []:
            return globals()[msg.get("method")]()
        else:
            # globals()[msg.get("method")](*msg.get("data"))
            return globals()[msg.get("method")](msg.get("data"))

    def write(self, string):
        if self.py4j:
            self.py4j.handleStdOut(string)

    def flush(self):
        pass            

    def start(self):
        print(f"[Py4jClient] Loading user module from: {self.script_path}")
        # Load the user script into the global namespace so its functions are available in globals()
        with open(self.script_path) as f:
            code = f.read()
        exec(code, globals())
        try:
            print("[Py4jClient] Attempting to connect to JavaGateway...")
            # self.gateway = JavaGateway(
            #     gateway_parameters=GatewayParameters(port=self.port),
            #     callback_server_parameters=None,
            #     python_server_entry_point=self
            # )

            # self.stdout = sys.stdout
            # self.stderr = sys.stderr
            # sys.stdout = self
            # sys.stderr = self
            self.gateway = JavaGateway(
                gateway_parameters=GatewayParameters(auto_convert=True),
                callback_server_parameters=CallbackServerParameters(),
                python_server_entry_point=self,
            )
            # self.runtime = self.gateway.jvm.org.myrobotlab.service.Runtime.getInstance()


            print("[Py4jClient] JavaGateway JVM:", self.gateway.jvm)
            print("[Py4jClient] Successfully connected to JavaGateway!")
        except Exception as e:
            print("[Py4jClient] Error connecting to JavaGateway:", e)
            sys.exit(1)
        self.running = True
        self.blocking_loop()

    def blocking_loop(self):
        print("[Py4jClient] Entering blocking loop. Press Ctrl+C to exit.")
        try:
            while self.running:
                try:
                    self.gateway.jvm.hashCode()
                    print("[Py4jClient] Connection alive.")
                except Exception as e:
                    print("[Py4jClient] Connection lost:", e)
                    break
                time.sleep(5)
        except KeyboardInterrupt:
            print("[Py4jClient] KeyboardInterrupt received, exiting.")
        print("[Py4jClient] Python script exiting.")

    def stop(self):
        print("[Py4jClient] Stopping client.")
        self.running = False
        if self.gateway is not None:
            self.gateway.shutdown()
            print("[Py4jClient] Gateway shutdown.")

if __name__ == "__main__":
    script_path = "/mnt/2tb/github/lucille/lucille-core/src/test/resources/PythonStageTest/process_document_1.py"  # Update as needed
    port = 25333  # Update as needed
    client = Py4jClient(script_path, port)
    client.start()
    # client.stop()

