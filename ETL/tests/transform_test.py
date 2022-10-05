import unittest
from datajob.etl.transform.tr_realestate_own import TrRealEstateOwnTransformer
from datajob.etl.transform.realestate_own import RealEstateOwnTransformer
from datajob.etl.transform.own_addr import OwnAddrTransformer
from datajob.etl.transform.own_sex_age import OwnSexAgeTransformer
from datajob.etl.transform.apartment_sale_price import ApartmentSalePriceTransformer
from datajob.etl.transform.local_code import LocalCodeTransformer

# test command : python3 -W ignore -m unittest tests.transform_test.MTest.test1
class MTest(unittest.TestCase):
    def test1(self):
        LocalCodeTransformer.transform()

    def test2(self):
        ApartmentSalePriceTransformer.transform()

    def test3(self):
        OwnSexAgeTransformer.transform()

    def test4(self):
        OwnAddrTransformer.transform(11)

    def test5(self):
        RealEstateOwnTransformer.transform()

    def test5(self):
        TrRealEstateOwnTransformer.transform()

   

if __name__ == "__main__":
    unittest.main()