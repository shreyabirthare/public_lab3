import requests
import unittest

#testing frontend microservice with various scenarios
class FrontEndServiceTest(unittest.TestCase):
    FRONT_END_URL = 'http://localhost:12503'

    def test_front_end_query_existing_product(self):
        response = requests.get(f'{self.FRONT_END_URL}/products/Tux')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('name', data['data'])
        self.assertEqual(data['data']['name'], 'Tux')

    def test_front_end_query_non_existing_product(self):
        response = requests.get(f'{self.FRONT_END_URL}/products/Crocodile')
        self.assertEqual(response.status_code, 404)

    def test_front_end_place_order_successfully(self):

        order_data = {'name': 'Python', 'quantity': 10}
        
        response = requests.post(f'{self.FRONT_END_URL}/orders/', json=order_data)
        self.assertEqual(response.status_code, 200)
        order_response_data = response.json()
        self.assertTrue('order_number' in order_response_data['data'])

    def test_front_end_quantity_more_than_available(self):
        order_data = {'name': 'Tux', 'quantity': 1000000}
        response = requests.post(f'{self.FRONT_END_URL}/orders/', json=order_data)
        self.assertEqual(response.status_code, 400)

    def test_front_end_place_order_for_non_existing_product(self):
        order_data = {'name': 'Caterpillar', 'quantity': 1}
        #order_data['leader'] ={'host': 'localhost', 'port': 12505}
        response = requests.post(f'{self.FRONT_END_URL}/orders/', json=order_data)
        self.assertNotEqual(response.status_code, 200)
        self.assertEqual(response.status_code, 404)


    def test_front_end_query_existing_order_number(self):
        order_number=0
        response = requests.get(f'{self.FRONT_END_URL}/orders/{order_number}')
        self.assertEqual(response.status_code, 200)

    def test_front_end_query_nonexisting_order_number(self):
        order_number=10000000000000
        response = requests.get(f'{self.FRONT_END_URL}/orders/{order_number}')
        self.assertEqual(response.status_code, 404)

    #assuming all order replicas are active
    def test_health_check(self):
        response = requests.get(f"http://localhost:12505/health")
        self.assertEqual(response.status_code, 200)

    def test_notify_leader_to_replica(self):
        url = f"http://localhost:12502/notify_leader_info_to_replica"
        leader_info={'host': 'localhost', 'port': 12505}
        leader_id=3
        data = {"leader": leader_info, "leader_id": leader_id}
        response = requests.post(url, json=data)
        self.assertEqual(response.status_code, 200)


#testing catalog microservice with various scenarios
class CatalogServiceTest(unittest.TestCase):
    CATALOG_URL = 'http://localhost:12501'

    def test_retrieve_product_info_directly(self):
        response = requests.get(f'{self.CATALOG_URL}/Tux')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('name', data)
        self.assertEqual(data['name'], 'Tux')


    def test_buy_product_successfully(self):
        buy_product_data = {'name': 'Fox', 'quantity': 1}
        response = requests.post(f'{self.CATALOG_URL}/buy', json=buy_product_data)
        self.assertEqual(response.status_code, 200)


    def test_buy_non_existent_product(self):
        buy_product_data = {'name': 'Caterpillar', 'quantity': 1}
        response = requests.post(f'{self.CATALOG_URL}/buy', json=buy_product_data)
        self.assertNotEqual(response.status_code, 200)

#testing order microservice with various scenarios
class OrderServiceTest(unittest.TestCase):
    ORDER_URL = 'http://localhost:12505'
    CATALOG_URL = 'http://localhost:12501'


    def test_place_order_successfully(self):
        order_data = {'name': 'Python', 'quantity': 10}
        order_data['leader'] ={'host': 'localhost', 'port': 12505}
        
        response = requests.post(f'{self.ORDER_URL}/orders', json=order_data)
        self.assertEqual(response.status_code, 200)
        order_response_data = response.json()
        self.assertTrue('order_number' in order_response_data)

    def test_query_existing_order_number(self):
        order_number=0
        response = requests.get(f'{self.ORDER_URL}/orders/{order_number}')
        self.assertEqual(response.status_code, 200)

    def test_query_nonexisting_order_number(self):
        order_number=10000000000000
        response = requests.get(f'{self.ORDER_URL}/orders/{order_number}')
        self.assertEqual(response.status_code, 404)

    def test_quantity_more_than_available(self):
        order_data = {'name': 'Tux', 'quantity': 1000000}
        order_data['leader'] ={'host': 'localhost', 'port': 12505}
        response = requests.post(f'{self.ORDER_URL}/orders', json=order_data)
        self.assertEqual(response.status_code, 400)
    
    def test_place_order_for_non_existing_product(self):
        order_data = {'name': 'Caterpillar', 'quantity': 1}
        order_data['leader'] ={'host': 'localhost', 'port': 12505}
        response = requests.post(f'{self.ORDER_URL}/orders', json=order_data)
        self.assertNotEqual(response.status_code, 200)
        self.assertEqual(response.status_code, 404)
    #assuming all order replicas are active
    def test_propagate_order_details_to_follower(self):
        data = {
        "order_number": 1000,
        "product_name": "Tux",
        "quantity": 10,
        "leader_id": f"localhost:12505"
        }
        url = f"http://localhost:12504/replicate_order"
        response = requests.post(url, json=data)
        self.assertEqual(response.status_code, 200)

    def test_missed_orders(self):
        url = f"http://localhost:12505/missed_order"
        response = requests.post(url, json={"latest_order_id": 0})
        self.assertTrue(response.status_code == 200 or response.status_code == 201)

    

if __name__ == '__main__':
    unittest.main()