from setuptools import setup, find_packages

setup(
    name="pystream",
    version="0.1",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=[
        # 你的依赖包
    ],
)
