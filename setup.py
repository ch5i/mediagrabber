from setuptools import setup, find_packages

setup (
   name='MediaGrabber',
   version='0.1',
   packages=find_packages(),
   description='A tool to automatically collect media files into a structure based on create date',

   # Declare your packages' dependencies here, for eg:
   install_requires=[],
   dependency_links=['https://github.com/smarnach/pyexiftool'],

   # Fill in these to make your Egg ready for upload to
   # PyPI
   author='Thomas Käser',
   author_email='tmail(at)kbox.ch',
   license='MIT',

   #summary = 'Just another Python package for the cheese shop',
   url='https://github.com/ch5i/MediaGrabber',
   long_description='Long description of the package',

   # could also include long_description, download_url, classifiers, etc.

   )