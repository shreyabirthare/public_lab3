import json
import random
import time
import requests
import os

FRONTEND_HOST = os.getenv('FRONTEND_HOST', 'localhost')
FORNT_END_PORT = int(os.getenv('FRONTEND_LISTENING_PORT',12503))

# Adjust this parameter to control the probability of placing an order
probability_order = 0.5
client_no=random.randint(1,1000)
# Function to perform a single session of queries and orders
def perform_session():
    # Open an HTTP connection with the front-end service
    session = requests.Session()
    # Start time
    startTime = time.time()
    # List of available products
    products = ["Tux", "Whale", "Fox", "Python"]

    for _ in range(50):  # Loop from 1 to 7
        # Randomly choose a product
        product = random.choice(products)

        # Query the availability of the chosen product
        response = session.get(f"http://{FRONTEND_HOST}:{FORNT_END_PORT}/products/{product}")

        # Print the data or error message from the query request
        try:
            response_data = response.json()
            print(f"Query result for {product}: {response_data}")
        except json.JSONDecodeError:
            print(f"Error for {product} query: {response.text}")

        # Check if the product is available and decide whether to place an order
        if response.status_code == 200:
            product_data = response.json()["data"]
            if product_data.get("quantity") > 0 and random.random() < probability_order:
                # Generate order data
                qty=random.randint(1, 50)
                order_data = {
                    "name": product,
                    "quantity": qty  # Order random quantity up to available quantity
                }
                print(f"placing order for {product} , {qty}")

                # Place an order
                order_response = session.post(f"http://{FRONTEND_HOST}:{FORNT_END_PORT}/orders/", json=order_data)

                # Print the data or error message from the order request
                try:
                    order_response_data = order_response.json()
                    print(f"Order result for {product}: {order_response_data}")
                except json.JSONDecodeError:
                    print(f"Error for {product} order: {order_response.text}")
    # End Time
    endTime = time.time()
    session.close()
    responseTime = endTime - startTime
    print(f"***Response Time: {responseTime} seconds***")

if __name__ == "__main__":
    perform_session()
