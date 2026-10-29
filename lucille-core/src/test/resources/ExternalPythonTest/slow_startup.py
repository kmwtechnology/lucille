"""
A Python script that delays startup to test timeout handling.
This script sleeps before printing the JavaGateway STARTED message,
allowing us to test the startup timeout logic.
"""
import time
import sys

print("Python process started, initializing...", flush=True)
print("Loading modules...", flush=True)
print("Configuring environment...", flush=True)

# Sleep for 5 seconds before reporting gateway ready
# The test will set timeout to 1 second to trigger timeout
time.sleep(5)

print("JavaGateway STARTED", flush=True)

def process_document(doc):
    """This will never be reached due to timeout"""
    return {"result": "should not execute"}