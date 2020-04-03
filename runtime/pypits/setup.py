from setuptools import setup, find_packages

setup (
       name='spits-runtime',
       version='2.0',
       packages=find_packages(),

       # Declare your packages' dependencies here, for eg:
       install_requires=['foo>=3'],

       # Fill in these to make your Egg ready for upload to
       # PyPI
       author='',
       author_email='',

       #summary = 'Just another Python package for the cheese shop',
       url='https://github.com/lmcad-unicamp/spits-2.0/',
       license='MIT',
       long_description='PYPITS 2.0: SPITS 2.0 runtime',

       # could also include long_description, download_url, classifiers, etc.
       scripts = [
            'scripts/install.sh'
       ]
)
