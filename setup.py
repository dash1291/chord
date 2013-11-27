from distutils.core import setup

setup(
    name='chorddht',
    version='0.1',
    packages=['chorddht',],
    scripts=['chorddht/bin/chordnode',],
    license='MIT',
    long_description=open('README.md').read(),
    install_requires=['zerorpc',],
)
