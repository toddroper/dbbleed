from setuptools import setup

packages = [
    "dbbleeder",
    "dbbleeder.commands",
    "dbbleeder.datatools"
]

setup(
    name="dbbleeder",
    version="0.1",
    packages=packages,
    entry_points={
        'console_scripts': [
            'dbbleed = dbbleeder.run:main'
        ]
    }
)
