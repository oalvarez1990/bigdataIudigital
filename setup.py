from setuptools import setup, find_packages

setup(
    name='proyecto_big_data',
    version='0.0.1',
    author='Omar',
    author_email='omaraleyser@hotmail.com',
    description='Proyecto de Big Data',
    py_modules=['proyecto'],
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'matplotlib',
        'seaborn',
        'scikit-learn',
        'joblib',             
        "openpyxl",
        "requests"
    ],    
    
)