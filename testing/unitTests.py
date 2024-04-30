import requests
import unittest

#testing frontend microservice with various scenarios
class FrontEndServiceTest(unittest.TestCase):
    FRONT_END_URL = 'http://localhost:12503'

    def test_query_existing_product(self):
        response = requests.get(f'{self.FRONT_END_URL}/products/Tux')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('name', data['data'])
        self.assertEqual(data['data']['name'], 'Tux')

    def test_query_non_existing_product(self):
        response = requests.get(f'{self.FRONT_END_URL}/products/Crocodile')
        self.assertEqual(response.status_code, 404)

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
    ORDER_URL = 'http://localhost:12502'
    CATALOG_URL = 'http://localhost:12501'

    def test_place_order_successfully(self):
        order_data = {'name': 'Python', 'quantity': 10}
        response = requests.post(f'{self.ORDER_URL}/orders', json=order_data)
        self.assertEqual(response.status_code, 200)
        order_response_data = response.json()
        self.assertTrue('order_number' in order_response_data)

    def test_quantity_more_than_available(self):
        response = requests.post(f'{self.ORDER_URL}/orders', json={'name': 'Tux', 'quantity': 500000})
        self.assertNotEqual(response.status_code, 200)
    
    def test_place_order_for_non_existing_product(self):
        response = requests.post(f'{self.ORDER_URL}/orders', json={'name': 'Caterpillar', 'quantity': 1})
        self.assertNotEqual(response.status_code, 200)
        self.assertEqual(response.status_code, 404)

if __name__ == '__main__':
    unittest.main()