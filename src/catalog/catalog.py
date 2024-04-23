import json
import threading
import requests
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse
import csv
import os
import time

# Initializing catalog service host and port, lock, catalog file
CATALOG_PORT = int(os.getenv('CATALOG_LISTENING_PORT', 12501))
CATALOG_FILE = "catalog_data/catalog.csv"
LOCK = threading.Lock()
CATALOG_HOST = os.getenv('CATALOG_HOST', 'localhost')
host = CATALOG_HOST
FRONTEND_HOST = os.getenv('FRONTEND_HOST', 'localhost')
FRONT_END_PORT = int(os.getenv('FRONTEND_LISTENING_PORT',12503))

# Making a public catalog dictionary
catalog = {}

def load_catalog():
    global catalog
    with LOCK:
        try:
            with open(CATALOG_FILE, 'r') as file:
                reader = csv.DictReader(file)
                catalog = {row['name']: {'price': float(row['price']), 'quantity': int(row['quantity'])} for row in reader}
        except FileNotFoundError:
            catalog = {
                "Tux": {"price": 15.99, "quantity": 100},
                "Whale": {"price": 25.99, "quantity": 100},
                "Fox": {"price": 12.99, "quantity": 100},
                "Python": {"price": 20.99, "quantity": 100},
                "Barbie": {"price": 55.99, "quantity": 100},
                "Lego": {"price": 45.99, "quantity": 100},
                "Monopoly": {"price": 10.99, "quantity": 100},
                "Frisbee": {"price": 5.99, "quantity": 100},
                "Marbles": {"price": 7.99, "quantity": 100},
                "Giraffe": {"price": 75.99, "quantity": 100}
            }
            with open(CATALOG_FILE, 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=['name', 'price', 'quantity'])
                writer.writeheader()
                for name, details in catalog.items():
                    writer.writerow({'name': name, 'price': details['price'], 'quantity': details['quantity']})

def send_invalidation_request(product_name):
    url = f"http://{FRONTEND_HOST}:{FRONT_END_PORT}/invalidate/{product_name}"
    try:
        response = requests.post(url)  # Added timeout for network calls
        if response.status_code == 200:
            response_data = response.json()
            print(f"{response_data}")
        else:
            error_response = response.json()
            print(f"{error_response}")
    except Exception as e:
        print(f"Unexpected error during invalidation request for {product_name}: {e}")

def restock_catalog():
    while True:
        time.sleep(10)
        with LOCK:
            updated = False
            for product, details in catalog.items():
                if details['quantity'] == 0:
                    print(f"Restocking {product}")
                    details['quantity'] = 100
                    updated = True
                    send_invalidation_request(product)  # Invalidate the front-end cache for this product
            if updated:
                with open(CATALOG_FILE, 'w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=['name', 'price', 'quantity'])
                    writer.writeheader()
                    for name, details in catalog.items():
                        writer.writerow({'name': name, 'price': details['price'], 'quantity': details['quantity']})

def handle_query(product_name):
    with LOCK:
        if product_name in catalog:
            product_info = catalog[product_name]
            response_data = {'name': product_name, 'price': product_info['price'], 'quantity': product_info['quantity']}
            return response_data, 200
        else:
            return None, 404

def update_catalog_for_buy(order_data):
    global catalog
    product_name = order_data.get("name")
    quantity = order_data.get("quantity")
    
    with LOCK:
        catalog[product_name]['quantity'] -= quantity
        # Update catalog CSV file with new corresponding quantity
        with open(CATALOG_FILE, 'r') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            
        for row in rows:
            if row['name'] == product_name:
                row['quantity'] = str(catalog[product_name]['quantity'])
            
        with open(CATALOG_FILE, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['name', 'price', 'quantity'])
            writer.writeheader()
            writer.writerows(rows)

        return 200

class CatalogRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        product_name = parsed_path.path.split("/")[-1]
        product_info, response_code = handle_query(product_name)
        self.send_response(response_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        if product_info:
            product_response = json.dumps(product_info)
            self.wfile.write(product_response.encode('utf-8'))

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        response_code = update_catalog_for_buy(post_data)
        self.send_response(response_code)
        if response_code == 200:
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
        else:
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(f"Updating catalog failed: {response_code}".encode('utf-8'))

def start_catalog_service():
    load_catalog()
    restock_thread = threading.Thread(target=restock_catalog, daemon=True)
    restock_thread.start()
    catalog_server = ThreadingHTTPServer((host, CATALOG_PORT), CatalogRequestHandler)
    print(f"Starting catalog service on {host}:{CATALOG_PORT}...")
    catalog_server.serve_forever()

if __name__ == "__main__":
    start_catalog_service()