import json
import random
import time
import requests
import os

FRONTEND_HOST = os.getenv('FRONTEND_HOST', 'localhost')
FRONT_END_PORT = int(os.getenv('FRONTEND_LISTENING_PORT', 12503))  # Corrected the typo in the variable name

# Adjust this parameter to control the probability of placing an order
probability_order = 0.4
client_no = random.randint(1, 1000)

time_buy_request=0.0
time_product_request=0.0
time_order_request=0.0
buy_counter=0
order_query_counter=0

# Function to perform a single session of queries and orders
def perform_session():
    session = requests.Session()
    global time_product_request, time_order_request, time_buy_request, buy_counter, order_query_counter

    #startTime = time.time()
    products = ["Tux", "Whale", "Fox", "Python", "Barbie", "Lego", "Monopoly", "Frisbee", "Marbles", "Giraffe"]
    order_numbers = []  # Store order numbers for later verification

    for _ in range(50):  # Loop from 1 to 50
        product = random.choice(products)
        #measuring response time for product query
        startTime_product_query = time.time()
        response = session.get(f"http://{FRONTEND_HOST}:{FRONT_END_PORT}/products/{product}", timeout=100)
        endTime_product_query = time.time()
        responseTime_product_query = endTime_product_query - startTime_product_query
        time_product_request=time_product_request+responseTime_product_query

        try:
            response_data = response.json()
            print(f"Query result for {product}: {response_data}")
            if response.status_code == 200 and response_data["data"]["quantity"] > 0 and random.random() < probability_order:
                qty = random.randint(1, 10)
                order_data = {"name": product, "quantity": qty}
                print(f"placing order for {product}, {qty}")
                buy_counter+=1
                #measuring response time for buy/purchase request
                startTime_buy_query = time.time()
                order_response = session.post(f"http://{FRONTEND_HOST}:{FRONT_END_PORT}/orders/", json=order_data, timeout=1000)
                endTime_buy_query = time.time()
                responseTime_buy_query = endTime_buy_query - startTime_buy_query
                time_buy_request=time_buy_request+responseTime_buy_query
                order_response_data = order_response.json()
                print(f"Order result for {product}: {order_response_data}")

                # Check if the 'data' key is present and has 'order_number' in the response
                if order_response_data.get('data') and 'order_number' in order_response_data['data']:
                    order_numbers.append(order_response_data['data']['order_number'])
                    order_query_counter+=1
                    #measuring response time for order query
                    startTime_order_query = time.time()
                    response1 = session.get(f"http://{FRONTEND_HOST}:{FRONT_END_PORT}/orders/{order_response_data['data']['order_number']}", timeout=1000)
                    endTime_order_query = time.time()
                    responseTime_order_query = endTime_order_query - startTime_order_query
                    time_order_request=time_order_request+responseTime_order_query
                else:
                    print(f"Error placing order for {product}: {order_response_data}")

                
        except json.JSONDecodeError:
            print(f"Error decoding JSON for {product} query: {response.text}")
        except KeyError:
            print(f"Unexpected JSON structure: {response.text}")
        time.sleep(10)

    # Verify each order's information after all are placed
    for order_number in order_numbers:
        order_info_response = session.get(f"http://{FRONTEND_HOST}:{FRONT_END_PORT}/orders/{order_number}", timeout=1000)
        order_info_data = order_info_response.json()
        if order_info_response.status_code == 200:
            print(f"Verified order {order_number}: {order_info_data}")
        else:
            print(f"Failed to retrieve order {order_number}, Error: {order_info_data}")

    #endTime = time.time()
    session.close()
    Final_product_request_responseTime = time_product_request/50
    Final_order_request_responseTime=0
    Final_buy_request_responseTime=0

    if time_order_request!=0:
        Final_order_request_responseTime = time_order_request/order_query_counter

    if time_buy_request!=0:
        Final_buy_request_responseTime = time_buy_request/buy_counter




    print(f"Results for buy/purchase probability: {probability_order*100}%")
    print(f"***Average Response Time Product Query: {Final_product_request_responseTime} seconds***")
    print(f"***Average Response Time Order Query: {Final_order_request_responseTime} seconds***")
    print(f"***Average Response Time Buy Request: {Final_buy_request_responseTime} seconds***")
    

if __name__ == "__main__":
    perform_session()
