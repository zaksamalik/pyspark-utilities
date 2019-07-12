import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyspark-utilities",
    version="0.0.1",
    author="Sam Zakalik",
    description="ETL focused utilities library for PySpark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/zaksamalik/pyspark-utilities",
    packages=setuptools.find_packages(),
    install_requires=['pyspark>=2.4.0',
                      'pandas>=0.24.3'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)