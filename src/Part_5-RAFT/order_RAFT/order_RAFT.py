import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse
import requests
import csv
import os

# Initializing order service host and port, lock, order file
Replica_id=int(os.getenv('Replica_id',1))
ORDER_PORT = int(os.getenv('ORDER_LISTENING_PORT', 12502))
CATALOG_PORT = int(os.getenv('CATALOG_LISTENING_PORT', 12501))
ORDER_FILE = f"order_data/order_log_{str(Replica_id)}.csv"
RAFT_FILE=f"raft_data/raft_log_{str(Replica_id)}.csv"
LOCK = threading.Lock()
CATALOG_HOST = os.getenv('CATALOG_HOST', 'localhost')
ORDER_HOST = os.getenv('ORDER_HOST', 'localhost')
#ORDER_NODES = os.getenv('ORDER_NODES', "localhost:12502,localhost:12504,localhost:12505")  # "host1:port1,host2:port2"
ORDER_NODES = {
    os.getenv('REPLICA1_ID', 1): {"id":1,"host": os.getenv('REPLICA1_HOST', 'localhost'), "port": int(os.getenv('REPLICA1_PORT', 12502))},
    os.getenv('REPLICA2_ID', 2): {"id":2,"host": os.getenv('REPLICA2_HOST', 'localhost'), "port": int(os.getenv('REPLICA2_PORT', 12504))},
    os.getenv('REPLICA3_ID', 3): {"id":3,"host": os.getenv('REPLICA3_HOST', 'localhost'), "port": int(os.getenv('REPLICA3_PORT', 12505))}
}
# Initializing global order number variable to 0
order_number = 0
raft_index=0
raft_term=0

def generate_order_number():
    with LOCK:
        global order_number
        order_number += 1
        return order_number - 1
        

def generate_raft_index():
    with LOCK:
        global raft_index
        raft_index += 1
        return raft_index - 1
'''
def decrement_raft_index():
    with LOCK:
        global raft_index
        raft_index -= 1
'''
def fetch_RAFT_TERM():
    global raft_term
    with LOCK:
        return raft_term
# Helper function to get follower details

def get_followers(leader_host, leader_port):
    return [follower for follower in ORDER_NODES.values() if follower['host'] != leader_host or follower['port'] != leader_port]

def send_data(follower, data):
    """Function to send data to a single follower."""
    url = f"http://{follower['host']}:{follower['port']}/replicate_order"
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()  # This will raise an exception for HTTP errors.
        print(f"Successfully propagated to {url}")
    except requests.RequestException as e:
        print(f"Error propagating to {url}: {e}")

def send_raft_data(follower, data):
    """Function to send data to a single follower."""
    url = f"http://{follower['host']}:{follower['port']}/replicate_raft"
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()  # This will raise an exception for HTTP errors.
        print(f"Successfully propagated raft entry to {url}")
        return 200
    except requests.RequestException as e:
        print(f"Error propagating raft entry to {url}: {e}")
        return 404
    
def send_invalidate_raft_index(follower, invalidate_index):
    """Function to send invalidate raft log enty request to a single follower."""
    url = f"http://{follower['host']}:{follower['port']}/invalidate_raft/{invalidate_index}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # This will raise an exception for HTTP errors.
        print(f"Successfully propagated invalidate raft entry request to {url}")
    except requests.RequestException as e:
        print(f"Error propagating  invalidate raft entry request to {url}: {e}")

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

def propagate_invalidate_raft_to_followers(invalidate_index, leader_info):

    threads = []
    for follower in get_followers(leader_info['host'], leader_info['port']):
        print(f"Trying to propagate raft entry to {follower['host']}")
        thread = threading.Thread(target=send_invalidate_raft_index, args=(follower, invalidate_index))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()  # Wait for all threads to complete

def propagate_raft_entry_to_followers(raft_index, raft_term, product_name, quantity, leader_info):
    data = {
        "raft_index": raft_index,
        "raft_term": raft_term,
        "product_name": product_name,
        "quantity": quantity,
        "leader_id": f"{ORDER_HOST}:{ORDER_PORT}"
    }
    vote=0
    for follower in get_followers(leader_info['host'], leader_info['port']):
        result=send_raft_data(follower, data)
        if result==200:
            vote+=1
            print(f"raft log received by {follower['host']}")
    print(f"this is the current vote status:{vote}")
    return vote


def log_order(order_number, product_name, quantity, leader_info=None):
    """Log order and optionally propagate to followers."""
    with LOCK:
        with open(ORDER_FILE, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([order_number, product_name, quantity])

        if leader_info:
            propagate_order_to_followers(order_number, product_name, quantity, leader_info)

def log_raft(given_raft_index, raft_term,product_name, quantity, leader_info=None):
    """Log raft entry and optionally propagate to followers."""
    global raft_index
    vote=0
    with LOCK:
        with open(RAFT_FILE, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([given_raft_index, raft_term, product_name, quantity])
        if leader_info:
            vote=propagate_raft_entry_to_followers(given_raft_index, raft_term, product_name, quantity, leader_info)
    if leader_info and vote==0:
        print("Did not get consensus from any follower-invalidating log now")
        invalidate_raft_index(raft_index)
        return 404
    elif leader_info and vote!=0:
        print("GOT POSITIVE CONSENSUS, CAN NOW PROCESS ORDER")
        return 200
    print("As a follower, updated raft log")
    return 200
                

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

def load_raft_index_number():
    global raft_index
    with LOCK:
        if os.path.exists(RAFT_FILE) and os.path.getsize(RAFT_FILE) > 0:
            with open(RAFT_FILE, 'r') as file:
                reader = csv.reader(file)
                last_row = list(reader)[-1]  # get latest order row
                raft_index = int(last_row[0]) + 1  # latest fetched order number incremented by 1
        else:
            raft_index = 0

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
        if available_quantity >= requested_quantity:
            return 200 #indicates product name exists and it is in stock
        else:
            return 400 #indicates that product name exists but quantity requested is not in stock
    else:
        return response.status_code #any other error code received from catalog like 404 out of stock or bad request
            

def fetch_latest_order_id():
    """Fetch the latest order ID"""
    with LOCK:
        local_latest_order_number=order_number
    if local_latest_order_number>0:
        return local_latest_order_number-1
    else:
        return local_latest_order_number

def fetch_latest_raft_id():
    """Fetch the latest order ID"""
    with LOCK:
        local_latest_raft_index=raft_index
    if local_latest_raft_index>0:
        return local_latest_raft_index-1
    else:
        return local_latest_raft_index

def invalidate_raft_index(given_raft_index):
    global raft_index
    # Read the CSV file
    with LOCK:
        with open(RAFT_FILE, 'r', newline='') as file:
            reader = csv.reader(file)
            rows = list(reader)
        # Iterate from the bottom to find the row with the specified raft_index
        for i in range(len(rows) - 1, -1, -1):
            if rows[i][0].startswith(str(given_raft_index-1)):
                del rows[i]
                break
        # Rewrite the CSV file without the specified row
        with open(RAFT_FILE, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
        if given_raft_index==raft_index:
            raft_index-=1
        print("INVALIDATED RAFT")

def request_missed_orders(order_number):
    """Request missed orders from the highest replica ID other than its own."""
    sorted_nodes = sorted(ORDER_NODES.values(), key=lambda x: x['id'], reverse=True)
    for node in sorted_nodes:
        if node['id'] != Replica_id:
            replica_host = node["host"]
            replica_port = node["port"]
            url = f"http://{replica_host}:{replica_port}/missed_order"
            try:
                response = requests.post(url, json={"latest_order_id": order_number})
                if response.status_code == 200:
                    print("No order is missed")
                    return
                elif response.status_code == 201:
                    # Collect missed orders into a list
                    missed_orders = response.json().get("missed_orders", [])
                    print(missed_orders)
                    if missed_orders:
                        with LOCK:
                            # Append new missed orders to the CSV file
                            with open(ORDER_FILE, 'a', newline='') as file:
                                writer = csv.writer(file)
                                for order in missed_orders:
                                    writer.writerow([order['order_number'], order['product_name'], order['quantity']])
                                print("appending missed orders")

                            # Read CSV file, sort its contents based on order_id, and overwrite the file
                            with open(ORDER_FILE, 'r', newline='') as file:
                                reader = csv.reader(file)
                                data = sorted(list(reader), key=lambda x: int(x[0]))  # Sort based on order_id
                            with open(ORDER_FILE, 'w', newline='') as file:
                                writer = csv.writer(file)
                                writer.writerows(data)
                                print(f"Missed orders received from replica {node['id']}")
                        load_order_number()
                    return
            except requests.RequestException as e:
                print(f"Error requesting missed orders from replica {node['id']}: {e}")
    print("Failed to receive missed orders from any replica")

def request_missed_raft_entries(raft_index):
    """Request missed orders from the highest replica ID other than its own."""
    sorted_nodes = sorted(ORDER_NODES.values(), key=lambda x: x['id'], reverse=True)
    for node in sorted_nodes:
        if node['id'] != Replica_id:
            replica_host = node["host"]
            replica_port = node["port"]
            url = f"http://{replica_host}:{replica_port}/missed_raft"
            try:
                response = requests.post(url, json={"latest_raft_id": raft_index})
                if response.status_code == 200:
                    print("No raft entry is missed")
                    return
                elif response.status_code == 201:
                    # Collect missed orders into a list
                    missed_raft_entries = response.json().get("missed_raft_entries", [])
                    print(missed_raft_entries)
                    if missed_raft_entries:
                        with LOCK:
                            # Append new missed orders to the CSV file
                            with open(RAFT_FILE, 'a', newline='') as file:
                                writer = csv.writer(file)
                                for missed_raft_entry in missed_raft_entries:
                                    writer.writerow([missed_raft_entry['raft_index'], missed_raft_entry['raft_term'],missed_raft_entry['product_name'], missed_raft_entry['quantity']])
                                print("appending missed raft entries")

                            # Read CSV file, sort its contents based on order_id, and overwrite the file
                            
                            with open(RAFT_FILE, 'r', newline='') as file:
                                reader = csv.reader(file)
                                data = sorted(list(reader), key=lambda x: int(x[0]))  # Sort based on raft_index
                            with open(RAFT_FILE, 'w', newline='') as file:
                                writer = csv.writer(file)
                                writer.writerows(data)
                                print(f"Missed raft entries received from replica {node['id']}")
                        load_raft_index_number()
                    return
            except requests.RequestException as e:
                print(f"Error requesting missed raft entries from replica {node['id']}: {e}")
    print("Failed to receive missed raft entries from any replica")

def fetch_missed_orders(start_order_id):
    """Fetch missed orders starting from the provided order ID."""
    missed_orders = []
    with LOCK:
        if os.path.exists(ORDER_FILE) and os.path.getsize(ORDER_FILE) > 0:
            with open(ORDER_FILE, 'r') as file:
                reader = csv.reader(file)
                found_start_id = False
                for row in reader:
                    if found_start_id:
                        missed_orders.append({"order_number": row[0], "product_name": row[1], "quantity": row[2]})
                    elif int(row[0]) == start_order_id:
                        found_start_id = True
    return missed_orders

def fetch_missed_raft_entries(start_raft_id):
    """Fetch missed raft entries starting from the provided raft ID."""
    missed_raft_entries = []
    with LOCK:
        if os.path.exists(RAFT_FILE) and os.path.getsize(RAFT_FILE) > 0:
            with open(RAFT_FILE, 'r') as file:
                reader = csv.reader(file)
                found_start_id = False
                for row in reader:
                    if found_start_id:
                        missed_raft_entries.append({"raft_index": row[0],"raft_term":row[1], "product_name": row[2], "quantity": row[3]})
                    elif int(row[0]) == start_raft_id:
                        found_start_id = True
    return missed_raft_entries
    


class OrderRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global raft_term
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "healthy"}).encode())
            print("i am now leader")
            return
        if self.path.split("/")[-2]=="note_raft_term":
            received_term= self.path.split("/")[-1]
            with LOCK:
                raft_term=received_term
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "RECEIVED RAFT TERM"}).encode())
            print("received raft term")
        
        if self.path.split("/")[-2]=="invalidate_raft":
            received_index= self.path.split("/")[-1]
            invalidate_raft_index(received_index)
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "Successfully invalidated RAFT entry"}).encode())
            print("Successfully invalidated RAFT entry")


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
        
        if self.path == "/replicate_raft":
            return self.handle_raft_replication()
        
        if self.path == "/missed_order":
            return self.handle_missed_order_request()
        
        if self.path == "/notify_leader_info_to_replica":
            return self.handle_leader_notification()
        
        if self.path == "/missed_raft":
            return self.handle_missed_raft_request()
        
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        product_name = post_data.get("name")
        requested_quantity = post_data.get("quantity")
        leader_info = post_data.get('leader')

        if not leader_info:
            self.send_response(403)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "This node is not the leader and cannot accept write operations. "}).encode())
            return
        
        product_status=check_product_availability(product_name, requested_quantity)

        # First check if the quantity is sufficient
        if product_status==200:
            raft_index_copy=generate_raft_index()
            raft_log_status=log_raft(raft_index_copy,fetch_RAFT_TERM(), product_name, requested_quantity,leader_info)
            if raft_log_status!=200:
                print(f" Not enough order nodes to process order for Requested qunatity {requested_quantity} for {product_name}")
                self.send_response(505)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                error_message = {"error": {"code": 505, "message": "Not enough order nodes available"}}
                self.wfile.write(json.dumps(error_message).encode())

            else:
                #then place order-item available
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
                #send catalog error in placing order
                else:
                    #send invalidate raft log request to followers
                    invalidate_raft_index(raft_index_copy)
                    propagate_invalidate_raft_to_followers(raft_index_copy,leader_info)
                    self.send_response(catalog_response.status_code)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    error_info = catalog_response.json()  # Assuming the catalog provides JSON error details
                    self.wfile.write(json.dumps(error_info).encode())
        #check if quantity is out of stock
        elif product_status==400:
            print(f"Requested qunatity {requested_quantity} is greater than available qunatity for {product_name}")
            self.send_response(400)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            error_message = {"error": {"code": 400, "message": "Insufficient product stock"}}
            self.wfile.write(json.dumps(error_message).encode())
        #else return bad request/wrong product name error
        else:
            print(f"Bad Request/Product name does not exist ")
            self.send_response(product_status)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            error_message = {"error": {"code": product_status, "message": "bad request/wrong product name"}}
            self.wfile.write(json.dumps(error_message).encode())
    
    def handle_replication(self):
        """Handle replication request from the leader."""
        data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        # Log the order without propagating since this is a follower action
        log_order(data['order_number'], data['product_name'], data['quantity'])
        received_order_id= int(data['order_number'])
        with LOCK:
            global order_number
            order_number =received_order_id+1
        
        print(f"Order replicated by leader ID: {data['leader_id']}")
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ORDER Replication successful"}).encode())

    def handle_raft_replication(self):
        """Handle raft entry replication request from the leader."""
        data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        # Log the order without propagating since this is a follower action
        replication_status=log_raft(data['raft_index'], data['raft_term'],data['product_name'], data['quantity'])
        received_raft_id= int(data['raft_index'])
        with LOCK:
            global raft_index
            raft_index =received_raft_id+1
            replication_status=201
        
        if(replication_status==201):
            print(f"Raft entry replicated by leader ID: {data['leader_id']}, replication successful")
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "RAFT Replication successful"}).encode())
    
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

    def handle_missed_order_request(self):
        """Handle request for missed orders from another replica."""
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        received_latest_order_id = post_data.get('latest_order_id')

        # Get the latest order ID
        local_latest_order_id = fetch_latest_order_id()

        if received_latest_order_id == local_latest_order_id:
            # If the received latest order ID matches the local latest order ID,
            # then respond with code 200, indicating that the local replica is up to date
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"message": "You are up to date with ORDERS"}).encode())
        elif received_latest_order_id < local_latest_order_id:
            # If the received latest order ID is less than the local latest order ID,
            # it means the other replica has missed some orders. Respond with code 201
            # and send the missed order details to the other replica.
            missed_orders = fetch_missed_orders(received_latest_order_id)
            self.send_response(201)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"missed_orders": missed_orders}).encode())

    def handle_missed_raft_request(self):
        """Handle request for missed raft entries from another replica."""
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        received_latest_raft_id = post_data.get('latest_raft_id')

        # Get the latest order ID
        local_latest_raft_id = fetch_latest_raft_id()

        if received_latest_raft_id == local_latest_raft_id:
            # If the received latest order ID matches the local latest order ID,
            # then respond with code 200, indicating that the local replica is up to date
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"message": "You are up to date with RAFT LOGS"}).encode())
        elif received_latest_raft_id < local_latest_raft_id:
            # If the received latest order ID is less than the local latest order ID,
            # it means the other replica has missed some orders. Respond with code 201
            # and send the missed order details to the other replica.
            missed_raft_entries = fetch_missed_raft_entries(received_latest_raft_id)
            self.send_response(201)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"missed_raft_entries": missed_raft_entries}).encode())


def start_order_service():
    load_order_number()  # Latest order number loaded from disk
    load_raft_index_number() # Latest raft index number loaded from disk
    request_missed_raft_entries(fetch_latest_raft_id())
    request_missed_orders(fetch_latest_order_id())
    order_server = ThreadingHTTPServer((ORDER_HOST, ORDER_PORT), OrderRequestHandler)
    print(f'Starting order service on {ORDER_HOST}:{ORDER_PORT}...')
    order_server.serve_forever()

if __name__ == "__main__":
    start_order_service()

#note to self: just change missed raft entries to synchronize with leader raft, send entire last raft log row.