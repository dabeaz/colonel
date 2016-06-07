from distutils.core import setup, Extension

setup(name='colonel',
      ext_modules=[
        Extension('colonel._colonel',
                  ['ext/_colonel.c'],
                  )
        ],
      packages=['colonel']
      )
