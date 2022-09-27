import unittest
from datajob.etl.extract.ownership_transfer_by_address import OwnershipTransferByAddress


class MTest(unittest.TestCase):

    def test1(self):
        OwnershipTransferByAddress.extract_data(10)


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
