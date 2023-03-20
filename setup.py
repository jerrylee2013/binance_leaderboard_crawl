import os
from setuptools import setup, find_packages

with open(
    os.path.join(os.path.dirname(__file__), "requirements/common.txt"), "r"
) as fh:
    requirements = fh.readlines()

NAME = "binance_leaderboard_crawl"
DESCRIPTION = "A crawl for binance leaderboard data"
AUTHOR = "Jerrylee2013"

VERSION = "1.0.0"

about = {}

# with open("README.md", "r") as fh:
#     about["long_description"] = fh.read()

root = os.path.abspath(os.path.dirname(__file__))


about["__version__"] = VERSION

setup(
    name=NAME,
    version=about["__version__"],
    license="MIT",
    description=DESCRIPTION,
    # long_description=about["long_description"],
    # long_description_content_type="text/markdown",
    AUTHOR=AUTHOR,
    # url=URL,
    keywords=["Crawl", "Binance"],
    install_requires=[req for req in requirements],
    packages=find_packages(exclude=("test",)),
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10"
    ],
    python_requires=">=3.6",
)
