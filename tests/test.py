import unittest


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    suite = unittest.TestLoader().discover('.')
    runner.run(suite)
