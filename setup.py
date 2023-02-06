from setuptools import setup, find_packages

with open("README.md", "r") as rf:
    long_description = rf.read()

setup(
    name="dressel",
    version="0.1.0",
    author="John Stanco",
    description="Dressel: GeoTIFF feature classification",
    long_description=long_description,
    packages=find_packages(),
    install_requires=[],
)
