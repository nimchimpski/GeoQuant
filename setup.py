from setuptools import setup, find_packages

setup(
    name="geoquant",
    version="0.1.0",
    description="Risk management and portfolio analytics for multi-currency portfolios.",
    author="Your Name",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas",
        "numpy",
        "matplotlib",
        "seaborn",
        "requests",
        "python-dotenv"
    ],
    python_requires=">=3.8",
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
