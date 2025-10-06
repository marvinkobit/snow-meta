from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='snowmeta',
    version='0.1.0',
    description='Snowmeta: a metadata-driven ingestion framework for Snowflake.',
    author='Maeruf',
    author_email='maeruf@nexsis.ca',
    license='MIT',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/marvinkobit/snow-meta',
    packages=find_packages(exclude=['tests', 'examples', 'build', 'dist', 'cursormd']),
    install_requires=[
        'snowflake-snowpark-python>=1.0.0',
        'pandas>=1.3.0',
        'pyyaml>=6.0',
    ],
    python_requires='>=3.9',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    keywords='snowflake snowpark data-engineering metadata',
    project_urls={
        'Documentation': 'https://github.com/marvinkobit/snow-meta',
        'Source': 'https://github.com/marvinkobit/snow-meta',
    },
)
