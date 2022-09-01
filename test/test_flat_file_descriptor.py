import unittest
from unittest.mock import patch


from flat_file_descriptor import FlatFileDescriptor


class FlatFileDescriptorTestCase(unittest.TestCase):
    
    def test_flat_file_descriptor(self):
        test_path = '/some/path/to/filename.csv'
        fd = FlatFileDescriptor(test_path)
        print(fd)



if __name__ == "__main__":
    unittest.main()
