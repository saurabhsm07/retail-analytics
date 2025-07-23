from setuptools import setup, find_packages

setup(
    name="retail-analytics",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark==3.5.6",
        "boto3",
        "requests",
    ],
    author="Saurabh Mahambrey",
    author_email="mahambrey.saurabh07@gmail.com",
    description="spark based analytics apps with random test code",
    include_package_data=True,
    zip_safe=False,
)