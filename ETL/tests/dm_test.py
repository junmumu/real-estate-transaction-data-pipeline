import unittest
from datajob.etl.datamart.sell_buy_ages import AccSellBuyAges, AccSellBuyAgesSido, SellBuyAgesYear
from datajob.etl.datamart.sell_buy_sex import AccSellBuySex,SellBuySexYear,AccSellBuySexSido
from datajob.etl.datamart.sell_buy_type import AccSellBuyType, SellBuyTypeYear, AccSellBuyTypeSido
from datajob.etl.datamart.sell_buy_foreign import AccSellBuyForeign, SellBuyForeignYear, AccSellBuyForeignSido
from datajob.datamart.monthly_apt_prc import MonthlyAptPrc


class MTest(unittest.TestCase):

    def test1(self):
        MonthlyAptPrc.save()

    def test2(self):
        AccSellBuyForeign.save()

    def test3(self):
        SellBuyForeignYear.save()

    def test4(self):
        AccSellBuyForeignSido.save()

    def test5(self):
        AccSellBuyType.save()

    def test6(self):
        SellBuyTypeYear.save()

    def test7(self):
        AccSellBuyTypeSido.save()

    def test8(self):
        AccSellBuySex.save()

    def test9(self):
        SellBuySexYear.save()

    def test10(self):
        AccSellBuySexSido.save()

    def test11(self):
        AccSellBuyAges.save()

    def test12(self):
        SellBuyAgesYear.save()

    def test13(self):
        AccSellBuyAgesSido.save()

if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
