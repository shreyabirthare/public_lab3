import json
import random
import time
import requests
import os

FRONTEND_HOST = os.getenv('FRONTEND_HOST', 'localhost')
FRONT_END_PORT = int(os.getenv('FRONTEND_LISTENING_PORT', 12503))  # Corrected the typo in the variable name

# Adjust this parameter to control the probability of placing an order
probability_order = 0.5
client_no = random.randint(1, 1000)

# Function to perform a single session of queries and orders
def perform_session():
    session = requests.Session()
    startTime = time.time()
    products = ["Tux", "Whale", "Fox", "Python", "Barbie", "Lego", "Monopoly", "Frisbee", "Marbles", "Giraffe"]
    order_numbers = []  # Store order numbers for later verification

    for _ in range(50):  # Loop from 1 to 50
        product = random.choice(products)
        response = session.get(f"http://{FRONTEND_HOST}:{FRONT_END_PORT}/products/{product}")

        try:
            response_data = response.json()
            print(f"Query result for {product}: {response_data}")
            if response.status_code == 200 and response_data["data"]["quantity"] > 0 and random.random() < probability_order:
                qty = random.randint(1, 10)
                order_data = {"name": product, "quantity": qty}
                print(f"placing order for {product}, {qty}")

                order_response = session.post(f"http://{FRONTEND_HOST}:{FRONT_END_PORT}/orders/", json=order_data)
                order_response_data = order_response.json()
                print(f"Order result for {product}: {order_response_data}")

                # Check if the 'data' key is present and has 'order_number' in the response
                if order_response_data.get('data') and 'order_number' in order_response_data['data']:
                    order_numbers.append(order_response_data['data']['order_number'])
                else:
                    print(f"Error placing order for {product}: {order_response_data.get('error', 'No error info provided')}")
        except json.JSONDecodeError:
            print(f"Error decoding JSON for {product} query: {response.text}")
        except KeyError:
            print(f"Unexpected JSON structure: {response.text}")

    # Verify each order's information after all are placed
    for order_number in order_numbers:
        order_info_response = session.get(f"http://{FRONTEND_HOST}:{FRONT_END_PORT}/orders/{order_number}")
        if order_info_response.status_code == 200:
            order_info_data = order_info_response.json()
            print(f"Verified order {order_number}: {order_info_data}")
        else:
            print(f"Failed to retrieve order {order_number}, Status code: {order_info_response.status_code}")

    endTime = time.time()
    session.close()
    responseTime = endTime - startTime
    print(f"***Response Time: {responseTime} seconds***")

if __name__ == "__main__":
    perform_session()
