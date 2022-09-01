import unittest
import pathlib


from flat_file import FlatFile


class DataProfilerTestCase(unittest.TestCase):
    
    def test_flat_file_descriptor(self):
        curr_dir = pathlib.Path(__file__).parent.resolve()
        test_file_path = '{0}/test_files/test_file.csv'.format(curr_dir)

        f = FlatFile(test_file_path)
        print(f.json())
        


if __name__ == "__main__":
    unittest.main()
