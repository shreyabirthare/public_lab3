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
ORDER_FILE = "order_data/order_log.csv"
LOCK = threading.Lock()
CATALOG_HOST = os.getenv('CATALOG_HOST', 'localhost')
ORDER_HOST = os.getenv('ORDER_HOST', 'localhost')
host = ORDER_HOST

# Initializing global order number variable to 0
order_number = 0

def generate_order_number():
    with LOCK:
        global order_number
        order_number += 1
        return order_number - 1

def log_order(order_number, product_name, quantity):
    with LOCK:
        with open(ORDER_FILE, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([order_number, product_name, quantity])

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
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        product_name = post_data.get("name")
        requested_quantity = post_data.get("quantity")

        # First check if the quantity is sufficient
        if check_product_availability(product_name, requested_quantity):
            #then place order
            print(f"{product_name} is in stock, placing order for {requested_quantity} quantity")
            catalog_response = requests.post(f"http://{CATALOG_HOST}:{CATALOG_PORT}/orders", json=post_data)
            if catalog_response.status_code == 200:
                order_number = generate_order_number()
                log_order(order_number, product_name, requested_quantity)
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


def start_order_service():
    load_order_number()  # Latest order number loaded from disk
    order_server = ThreadingHTTPServer((host, ORDER_PORT), OrderRequestHandler)
    print(f'Starting order service on {host}:{ORDER_PORT}...')
    order_server.serve_forever()

if __name__ == "__main__":
    start_order_service()
