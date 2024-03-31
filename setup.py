from setuptools import setup, find_packages


setup(
    name='mdm-python',
    version='0.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    package_data={"mdm_python.backend_server": ["templates/*.html", "static/scripts/*.js", "static/styles/*.css"]},
    setup_requires = ["setuptools-scm"],
    install_requires=[
        "anyio",
        "beautifulsoup4",
        "httpx",
        "pandas",
        "pymongo",
        "bokeh",
        "statsmodels",
    ],
    
    author='Daniela Komenda',
    author_email='komenda.daniela@gmail.com',
    description='Python-Project for MDM-Course at ZHAW - FS24',
)
