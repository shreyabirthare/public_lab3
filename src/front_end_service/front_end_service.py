import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse
import threading
import requests
import os
from collections import OrderedDict

#initializing front_end_service host and port
FRONT_END_PORT = int(os.getenv('FRONTEND_LISTENING_PORT',12503))
CATALOG_PORT = int(os.getenv('CATALOG_PORT',12501))
ORDER_PORT = int(os.getenv('ORDER_PORT',12502))

FRONTEND_HOST = os.getenv('FRONTEND_HOST', 'localhost')
CATALOG_HOST = os.getenv('CATALOG_HOST', 'localhost')
ORDER_HOST = os.getenv('ORDER_HOST', 'localhost')

class LRUCache:
    """ LRU Cache to hold the product data with thread-safe operations """
    def __init__(self, capacity=5):
        self.cache = OrderedDict()
        self.capacity = capacity
        self.lock = threading.Lock()  # Add a lock for thread safety

    def get(self, key):
        with self.lock:  # Use the lock when accessing the cache
            if key not in self.cache:
                return None
            else:
                self.cache.move_to_end(key)  # Mark as recently used
                return self.cache[key]

    def put(self, key, value):
        with self.lock:  # Use the lock when modifying the cache
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)  # Remove least recently used item

    def invalidate(self, key):
        with self.lock:  # Use the lock when modifying the cache
            if key in self.cache:
                del self.cache[key]
                print(f"Cache successfully invalidated for {key}")
            else:
                print(f"No cache entry found for {key} to invalidate.")

class FrontendHandler(BaseHTTPRequestHandler):
    #method to handle all get requests from client. requests forwarded to catalog service

    cache = LRUCache()  # Initialize the cache

    def do_GET(self):
        print(f"Thread ID {threading.get_ident()} handling request from {self.client_address}")
        parsed_path = urllib.parse.urlparse(self.path)

        #if query product
        if parsed_path.path.startswith("/products/"):
            product_name = parsed_path.path.split("/")[-1]
            product_info = self.cache.get(product_name)  # Try to get product info from cache
            if product_info:
                # Cache hit
                print("***** CACHE HIT *****")
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"data": product_info}).encode('utf-8'))
            else:
                # Cache miss
                print("***** CACHE MISS *****")
                request = requests.get(f"http://{CATALOG_HOST}:{CATALOG_PORT}/{product_name}")
                #return catalog response to client.
                if request.status_code==200:   #sends product info in data label if query was successful
                    product_info = request.json()
                    # Store to cache
                    self.cache.put(product_name, product_info)
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"data": product_info}).encode('utf-8')) 
                elif request.status_code==404: #sends error code in error label with corresponding message
                    self.send_response(404)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    error_message = {"error": {"code": 404, "message": "product not found"}} 
                    self.wfile.write(json.dumps(error_message).encode('utf-8'))
                else:   #sends error code in error label with corresponding message
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    error_message = {"error": {"code": 400, "message": "bad request"}}
                    self.wfile.write(json.dumps(error_message).encode('utf-8'))
        #else if query order info
        elif parsed_path.path.startswith("/orders/"):
            order_number = parsed_path.path.split("/")[-1]
            order_info = requests.get(f"http://{ORDER_HOST}:{ORDER_PORT}/orders/{order_number}")
            #return order response
            if order_info.status_code == 200:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"data": order_info.json()}).encode('utf-8'))
            elif order_info.status_code == 404:
                self.send_response(404)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                error_message = {"error": {"code": 404, "message": "Order not found"}}
                self.wfile.write(json.dumps(error_message).encode('utf-8'))
            else:
                self.send_response(400)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                error_message = {"error": {"code": 400, "message": "Bad request"}}
                self.wfile.write(json.dumps(error_message).encode('utf-8'))

    #method to handle all post requests from client. requests forwarded to order service
    def do_POST(self):
        print(f"Thread ID {(threading.get_ident())} handling request from {self.client_address}")
        parsed_path = urllib.parse.urlparse(self.path)
        #place orders, forward to order service
        if parsed_path.path.startswith("/orders/"):
            order_data = json.loads(self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8'))
            try:
                order_info = requests.post(f"http://{ORDER_HOST}:{ORDER_PORT}/orders", json=order_data)
                if order_info.status_code==200: #sends order info in data label if query was successful
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"data": order_info.json()}).encode('utf-8'))
                elif order_info.status_code==404:   #sends error code in error label with corresponding message
                    self.send_response(404)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    error_message = {"error": {"code": 404, "message": "product not found or is out of stock"}}
                    self.wfile.write(json.dumps(error_message).encode('utf-8'))
            except: #sends error code in error label with corresponding message for all other errors
                self.send_response(400)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                error_message = {"error": {"code": 400, "message": "bad request"}}
                self.wfile.write(json.dumps(error_message).encode('utf-8'))
        #handle invaldate requests
        elif parsed_path.path.startswith("/invalidate/"):
            product_name = parsed_path.path.split("/")[-1]
            self.cache.invalidate(product_name)

# host = 'localhost'
host = FRONTEND_HOST
port = FRONT_END_PORT

frontend_server = ThreadingHTTPServer((host, port), FrontendHandler)

print(f'Starting front-end server on {host}:{port}...')
frontend_server.serve_forever()
