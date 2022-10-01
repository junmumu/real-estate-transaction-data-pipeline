import unittest

from datajob.datamart.monthly_apt_prc import MonthlyAptPrc


class MTest(unittest.TestCase):

    def test1(self):
        MonthlyAptPrc.save()



if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
