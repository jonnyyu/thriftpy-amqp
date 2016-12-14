from setuptools import setup

setup(name='thriftpy-amqp',
      version='0.1',
      description='thriftpy amqp transport',
      url='http://github.com/jonnyyu/thriftpy-amqp',
      author='Jonny Yu',
      author_email='yingshen.yu@gmail.com',
      license='MIT',
      packages=['thriftpy_amqp'],
      install_requires=[
          'pika == 0.10.0',
          'thriftpy == 0.3.9',
      ],
      zip_safe=False)
