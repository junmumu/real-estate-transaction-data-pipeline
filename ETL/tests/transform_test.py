import unittest
from datajob.etl.transform.apartment_sale_price import ApartmentSalePriceTransformer
from datajob.etl.transform.local_code import LocalCodeTransformer

class MTest(unittest.TestCase):

    def test1(self):
        LocalCodeTransformer.transform()

    def test2(self):
        ApartmentSalePriceTransformer.transform()



if __name__ == "__main__":
    unittest.main()