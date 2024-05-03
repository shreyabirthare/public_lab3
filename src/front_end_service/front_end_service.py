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
FRONTEND_HOST = os.getenv('FRONTEND_HOST', '0.0.0.0')
CATALOG_HOST = os.getenv('CATALOG_HOST', 'localhost')

# Configuration of Order Service Replicas
ORDER_REPLICAS = {
    os.getenv('REPLICA1_ID', 1): {"host": os.getenv('REPLICA1_HOST', 'localhost'), "port": int(os.getenv('REPLICA1_PORT', 12502))},
    os.getenv('REPLICA2_ID', 2): {"host": os.getenv('REPLICA2_HOST', 'localhost'), "port": int(os.getenv('REPLICA2_PORT', 12504))},
    os.getenv('REPLICA3_ID', 3): {"host": os.getenv('REPLICA3_HOST', 'localhost'), "port": int(os.getenv('REPLICA3_PORT', 12505))}
}

def notify_replica(replica, leader_info, leader_id):
    """Function to send leader notification to a single replica and handle all responses."""
    url = f"http://{replica['host']}:{replica['port']}/notify_leader_info_to_replica"
    data = {"leader": leader_info, "leader_id": leader_id}
    try:
        response = requests.post(url, json=data)
        response_data = response.json()
        if response.status_code == 200:
            print(f"Successfully notified {url}. Leader ID: {leader_id} accepted. {response_data}")
        else:
            print(f"Failed to notify {url}. Status code: {response.status_code}, Error: {response_data}")
    except requests.ConnectionError:
        print(f"ConnectionError: Could not connect to {url}")
    except requests.Timeout:
        print(f"TimeoutError: Timeout while trying to connect to {url}")
    except requests.RequestException as e:
        print(f"HTTPError: An error occurred while notifying {url}. Error: {str(e)}")
    except json.JSONDecodeError:
        print(f"JSONError: Failed to decode JSON response from {url}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")

def notify_replicas_of_leader(leader_info, replicas, leader_id):
    """ Notify all replicas of the current leader along with the leader's ID. """
    threads = []
    for replica in replicas:
        if f"{replica['host']}:{replica['port']}" != f"{leader_info['host']}:{leader_info['port']}":
            thread = threading.Thread(target=notify_replica, args=(replica, leader_info, leader_id))
            thread.start()
            threads.append(thread)
    
    for thread in threads:
        thread.join()  # Wait for all threads to complete

def get_leader():
    """ Find the leader, notify others, and include the leader ID. """
    for replica_id in sorted(ORDER_REPLICAS.keys(), reverse=True):
        replica = ORDER_REPLICAS[replica_id]
        try:
            response = requests.get(f"http://{replica['host']}:{replica['port']}/health")
            response_data = response.json()
            if response.status_code == 200:
                print(f"Leader found: Order Service {replica_id}. {response_data}")
                # Notify other replicas about the leader and send the leader's replica ID
                notify_replicas_of_leader(replica, ORDER_REPLICAS.values(), replica_id)
                return replica
        except requests.ConnectionError:
            print(f"Failed to connect to Order Service {replica_id} at {replica['host']}:{replica['port']}")
    return None

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
            leader = get_leader()
            if leader is None:
                self.send_response(503)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                error_message = {"error": {"code": 503, "message": "Order service unavailable. No leader found."}}
                self.wfile.write(json.dumps(error_message).encode())
                return
            order_number = parsed_path.path.split("/")[-1]
            order_info = requests.get(f"http://{leader['host']}:{leader['port']}/orders/{order_number}")
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
            leader = get_leader()
            if leader is None:
                self.send_response(503)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                error_message = {"error": {"code": 503, "message": "Service unavailable. No leader found."}}
                self.wfile.write(json.dumps(error_message).encode())
                return
            order_data = json.loads(self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8'))
            order_data['leader'] = leader
            try:
                order_info = requests.post(f"http://{leader['host']}:{leader['port']}/orders", json=order_data)
                if order_info.status_code==200: #sends order info in data label if query was successful
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"data": order_info.json()}).encode('utf-8'))
                else:   #sends error code in error label with corresponding message
                    self.send_response(order_info.status_code)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    error_message = {"error": {"code": 404, "message": order_info.json()}}
                    self.wfile.write(json.dumps(error_message).encode('utf-8'))
            except: #sends error code in error label with corresponding message for all other errors
                self.send_response(400)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                error_message = {"error": {"code": 400, "message": "bad request"}}
                self.wfile.write(json.dumps(error_message).encode('utf-8'))
        # handle invalidate requests
        elif parsed_path.path.startswith("/invalidate/"):
            product_name = parsed_path.path.split("/")[-1]
            try:
                self.cache.invalidate(product_name)
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                response_content = json.dumps({"data": f"Cache successfully invalidated for {product_name}"})
                self.wfile.write(response_content.encode('utf-8'))
            except KeyError as e:
                self.send_response(404)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                error_message = json.dumps({"error": {"code": 404, "message": f"No cache entry found for {product_name} to invalidate."}})
                self.wfile.write(error_message.encode('utf-8'))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                error_message = json.dumps({"error": {"code": 500, "message": f"An error occurred while invalidating the cache for {product_name}: {str(e)}"}})
                self.wfile.write(error_message.encode('utf-8'))


frontend_server = ThreadingHTTPServer((FRONTEND_HOST, FRONT_END_PORT), FrontendHandler)
print(f'Starting front-end server on {FRONTEND_HOST}:{FRONT_END_PORT}...')
frontend_server.serve_forever()
