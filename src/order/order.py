import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse
import requests
import csv
import os

# Initializing order service host and port, lock, order file
ORDER_PORT = int(os.getenv('ORDER_LISTENING_PORT', 12502))
CATALOG_PORT = int(os.getenv('CATALOG_LISTENING_PORT', 12501))
ORDER_FILE = f"order_data/order_log_{os.getenv('REPLICA_ID', '1')}.csv"
LOCK = threading.Lock()
CATALOG_HOST = os.getenv('CATALOG_HOST', 'localhost')
ORDER_HOST = os.getenv('ORDER_HOST', 'localhost')
ORDER_NODES = os.getenv('ORDER_NODES', "localhost:12502,localhost:12504,localhost:12505")  # "host1:port1,host2:port2"

# Initializing global order number variable to 0
order_number = 0

def generate_order_number():
    with LOCK:
        global order_number
        order_number += 1
        return order_number - 1
    
# Helper function to get follower details
def get_followers(leader_host, leader_port):
    # Exclude the leader node based on host and port information
    return [dict(zip(["host", "port"], f.split(":"))) 
            for f in ORDER_NODES.split(",") 
            if f != f"{leader_host}:{leader_port}"]

def send_data(follower, data):
    """Function to send data to a single follower."""
    url = f"http://{follower['host']}:{follower['port']}/replicate_order"
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()  # This will raise an exception for HTTP errors.
        print(f"Successfully propagated to {url}")
    except requests.RequestException as e:
        print(f"Error propagating to {url}: {e}")

def propagate_order_to_followers(order_number, product_name, quantity, leader_info):
    data = {
        "order_number": order_number,
        "product_name": product_name,
        "quantity": quantity,
        "leader_id": f"{ORDER_HOST}:{ORDER_PORT}"
    }
    threads = []
    for follower in get_followers(leader_info['host'], leader_info['port']):
        thread = threading.Thread(target=send_data, args=(follower, data))
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()  # Wait for all threads to complete

def log_order(order_number, product_name, quantity, leader_info=None):
    """Log order and optionally propagate to followers."""
    with LOCK:
        with open(ORDER_FILE, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([order_number, product_name, quantity])
        if leader_info:
            propagate_order_to_followers(order_number, product_name, quantity, leader_info)

def load_order_number():
    global order_number
    with LOCK:
        if os.path.exists(ORDER_FILE) and os.path.getsize(ORDER_FILE) > 0:
            with open(ORDER_FILE, 'r') as file:
                reader = csv.reader(file)
                last_row = list(reader)[-1]  # get latest order row
                order_number = int(last_row[0]) + 1  # latest fetched order number incremented by 1
        else:
            order_number = 0

def fetch_order_details(order_number):
    with LOCK:
        with open(ORDER_FILE, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                if int(row[0]) == order_number:
                    return {"number": row[0], "name": row[1], "quantity": row[2]}
    return None

def check_product_availability(product_name, requested_quantity):
    """Check if the catalog has enough quantity of the product."""
    response = requests.get(f"http://{CATALOG_HOST}:{CATALOG_PORT}/{product_name}")
    if response.status_code == 200:
        product_data = response.json()
        available_quantity = product_data['quantity']
        return available_quantity >= requested_quantity
    return False

class OrderRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "healthy"}).encode())
            return

        order_number = self.path.split("/")[-1]
        try:
            order_number = int(order_number)
            order_data = fetch_order_details(order_number)
            if order_data:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"data": order_data}).encode())
            else:
                self.send_response(404)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                error_message = {"error": {"code": 404, "message": "Order not found"}}
                self.wfile.write(json.dumps(error_message).encode())
        except ValueError:
            self.send_response(400)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            error_message = {"error": {"code": 400, "message": "Invalid order number"}}
            self.wfile.write(json.dumps(error_message).encode())

    def do_POST(self):

        if self.path == "/replicate_order":
            return self.handle_replication()
        
        if self.path == "/notify_leader":
            return self.handle_leader_notification()
        
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        product_name = post_data.get("name")
        requested_quantity = post_data.get("quantity")
        leader_info = post_data.get('leader')

        if not leader_info:
            self.send_response(403)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "This node is not the leader and cannot accept write operations."}).encode())
            return

        # First check if the quantity is sufficient
        if check_product_availability(product_name, requested_quantity):
            #then place order
            print(f"{product_name} is in stock, placing order for {requested_quantity} quantity")
            catalog_response = requests.post(f"http://{CATALOG_HOST}:{CATALOG_PORT}/orders", json=post_data)
            if catalog_response.status_code == 200:
                order_number = generate_order_number()
                log_order(order_number, product_name, requested_quantity, leader_info)
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                response_data = {"order_number": order_number}
                self.wfile.write(json.dumps(response_data).encode())
            else:
                self.send_response(catalog_response.status_code)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                error_info = catalog_response.json()  # Assuming the catalog provides JSON error details
                self.wfile.write(json.dumps(error_info).encode())
        else:
            print(f"Requested qunatity {requested_quantity} is greater than available qunatity for {product_name}")
            self.send_response(400)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            error_message = {"error": {"code": 400, "message": "Insufficient product stock"}}
            self.wfile.write(json.dumps(error_message).encode())
    
    def handle_replication(self):
        """Handle replication request from the leader."""
        data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        # Log the order without propagating since this is a follower action
        log_order(data['order_number'], data['product_name'], data['quantity'])
        print(f"Order replicated by leader ID: {data['leader_id']}")
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "Replication successful"}).encode())
    
    def handle_leader_notification(self):
        """Handle leader notification request from the front-end service."""
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        leader_info = post_data.get('leader')
        leader_id = post_data.get('leader_id')

        if leader_info and leader_id is not None:
            print(f"Found Leader! Leader ID: {leader_id} and Leader Info: {leader_info}")
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "Leader updated successfully"}).encode())
        else:
            self.send_response(400)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "No leader information provided"}).encode())


def start_order_service():
    load_order_number()  # Latest order number loaded from disk
    order_server = ThreadingHTTPServer((ORDER_HOST, ORDER_PORT), OrderRequestHandler)
    print(f'Starting order service on {ORDER_HOST}:{ORDER_PORT}...')
    order_server.serve_forever()

if __name__ == "__main__":
    start_order_service()
