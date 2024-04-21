import json
import threading
import requests
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse
import csv
import os

#initializing catalog_service host and port, lock, catalog file
CATALOG_PORT = int(os.getenv('CATALOG_LISTENING_PORT',12501))
CATALOG_FILE = "catalog_data/catalog.csv"
LOCK = threading.Lock()
CATALOG_HOST = os.getenv('CATALOG_HOST', 'localhost')
# host='localhost'
host = CATALOG_HOST
# making a public catalog dictionary
catalog = {}

# method to load catalog from disk every time server starts.
def load_catalog():
    global catalog
    with LOCK:
        try:
            with open(CATALOG_FILE, 'r') as file:
                reader = csv.DictReader(file)
                catalog = {row['name']: {'price': float(row['price']), 'quantity': int(row['quantity'])} for row in reader}
        except FileNotFoundError:
            # If the file does not exist, set the catalog with these default values
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
            # default catalog is saved to new file
            with open(CATALOG_FILE, 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=['name', 'price', 'quantity'])
                writer.writeheader()
                for name, details in catalog.items():
                    writer.writerow({'name': name, 'price': details['price'], 'quantity': details['quantity']})

# method to return data for product query requests
def handle_query(product_name):
    with LOCK:
        
        if product_name in catalog:
            product_info = catalog[product_name]
            response_data = {'name': product_name, 'price': product_info['price'], 'quantity': product_info['quantity']}
            return response_data, 200
        else:
            return None, 404

# method to handle all buy requests and update catalog accordingly
def handle_buy(order_data):
    global catalog
    product_name = order_data.get("name")
    quantity = order_data.get("quantity")
    
    if not product_name or not quantity:
        print("product not found/bad req")
        return 400
    
    with LOCK:
        if product_name in catalog and catalog[product_name]['quantity'] >= quantity:
            print("product is in stock, updating catalog")
            catalog[product_name]['quantity'] -= quantity  # Subtract the requested quantity from catalog
            # Update catalog CSV file with new corresponding quantity
            with open(CATALOG_FILE, 'r') as file:
                reader = csv.DictReader(file)
                rows = list(reader)

            # corresponding row to the product name is found and its quantity is updated
            for row in rows:
                if row['name'] == product_name:
                    row['quantity'] = str(int(row['quantity']) - int(quantity))
                    break

            # modified rows written back to the catalog disk csv file
            with open(CATALOG_FILE, 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=['name', 'price', 'quantity'])
                writer.writeheader()
                writer.writerows(rows)
               
            return 200
        else:
            return 404

# Catalog Request Handler
class CatalogRequestHandler(BaseHTTPRequestHandler):
    #handle get requests by calling handle_query method, responses sent to frontend service
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
    #handle get requests by calling handle_buy method, responses sent to order service
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        response_code = handle_buy(post_data)
        self.send_response(response_code)

        if response_code == 200:
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
        else:
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(f"Order failed: {response_code}".encode('utf-8'))

# Start the catalog_service
def start_catalog_service():
    load_catalog()  # Loading the catalog data each time service is restarted
    catalog_server = ThreadingHTTPServer((host,CATALOG_PORT), CatalogRequestHandler)
    print(f'Starting catalog service on port {CATALOG_PORT}...')
    catalog_server.serve_forever()

if __name__ == "__main__":
    start_catalog_service()