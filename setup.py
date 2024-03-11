from setuptools import setup, find_packages

# File to install the project as a Library

setup(
    name='mdm-python',
    version='0.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        "anyio",
        "beautifulsoup4",
        "httpx",
        "pandas",
        "pymongo",
    ],
    
    author='Daniela Komenda',
    author_email='komenda.daniela@gmail.com',
    description='Python-Project for MDM-Course at ZHAW - FS24',
)
