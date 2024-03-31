from setuptools import setup, find_packages


setup(
    name='mdm-python',
    version='0.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        "anyio==4.3.0",
        "beautifulsoup4==4.12.3",
        "httpx==0.27.0",
        "pandas==2.2.1",
        "pymongo==4.6.2",
        "bokeh==3.3.4",
        "statsmodels==0.14.1",
    ],
    
    author='Daniela Komenda',
    author_email='komenda.daniela@gmail.com',
    description='Python-Project for MDM-Course at ZHAW - FS24',
)
