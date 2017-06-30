import unittest
import sys


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    suite = unittest.TestLoader().discover('.')
    exit_code = not runner.run(suite).wasSuccessful()
    sys.exit(exit_code)
