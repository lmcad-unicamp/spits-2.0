from setuptools import setup, find_packages

setup (
       name='spitz-python',
       version='0.2',
       packages=find_packages(),

       # Declare your packages' dependencies here, for eg:
       install_requires=['foo>=3'],

       # Fill in these to make your Egg ready for upload to
       # PyPI
       author='',
       author_email='',

       #summary = 'Just another Python package for the cheese shop',
       url='https://github.com/hpg-cepetro/spitz/',
       license='MIT',
       long_description='Long description of the package',

       # could also include long_description, download_url, classifiers, etc.
       scripts = [
            'scripts/install.sh'
       ]
)
