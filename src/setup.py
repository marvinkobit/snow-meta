from setuptools import setup, find_packages

setup(
    name='snowmeta',
    version='0.1.0',
    description='Snowflake metadata driven framework',
    author='Marvinkobit',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[],
    python_requires='>=3.7',
)
