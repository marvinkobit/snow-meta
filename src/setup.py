from setuptools import setup, find_packages

setup(
    name='snowmeta',
    version='0.2.0',
    description='Snowflake metadata driven framework with Snowpark support',
    author='Marvinkobit',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'snowflake-snowpark-python>=1.0.0',
        'pandas>=1.3.0',
        'pyyaml>=6.0',
    ],
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    keywords='snowflake snowpark data-engineering metadata',
    project_urls={
        'Documentation': 'https://github.com/your-org/snow-meta',
        'Source': 'https://github.com/your-org/snow-meta',
    },
)
