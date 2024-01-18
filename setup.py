from setuptools import setup, find_packages

setup(
    name='zsu',
    packages=find_packages(),
    url='https://github.com/suxxx313/zsu',
    license='',
    author='Shawn Su',
    author_email='suzixiang91@gmail.com',
    description='',
    use_scm_version=True,
    setup_requires=[
        'setuptools_scm',
    ],
    package_data={'': ['*.txt']},
    install_requires=[
        'xgboost>=1.2.0',
        'numpy',
        'pandas>=0.20',
        'ipykernel>=5.0',
        'sklearn',
        'pyarrow'
    ],
    tests_require=[
        'pytest',
    ],
    include_package_data=True,
    zip_safe=True,
)
