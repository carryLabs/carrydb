:man_page: bson_installing

Installing libbson
==================

The following guide will step you through the process of downloading, building, and installing the current release of the libbson.

.. _installing_supported_platforms:

Supported Platforms
-------------------

The library is continuously tested on GNU/Linux, Windows 7, Mac OS X 10.10, and Solaris 11 (Intel and Sparc), with GCC, Clang, and Visual Studio 2010, 2013, and 2015.

The library supports the following operating systems and CPU architectures:

=======================  =================  ======================================
Operating Systems        CPU Architectures  Compiler Toolchain
=======================  =================  ======================================
GNU/Linux                x86 and x86_64     GCC 4.1 and newer
Solaris 11               ARM                Clang 3.3 and newer
Mac OS X 10.6 and newer  PPC                Microsoft Visual Studio 2010 and newer
Windows Vista, 7, and 8  SPARC              `Oracle Solaris Studio 12`_
FreeBSD                                     MinGW
=======================  =================  ======================================

.. _Oracle Solaris Studio 12: http://www.oracle.com/technetwork/server-storage/solarisstudio/downloads/index.html

Install with a Package Manager
------------------------------

The libbson package is available on recent versions of Debian and Ubuntu.

.. code-block:: none

  $ apt-get install libbson-1.0

On Fedora, a libbson package is available in the default repositories and can be installed with:

.. code-block:: none

  $ dnf install libbson

On recent Red Hat systems, such as CentOS and RHEL 7, a libbson package
is available in the `EPEL <https://fedoraproject.org/wiki/EPEL>`_ repository. To check
version available, see `https://apps.fedoraproject.org/packages/libbson <https://apps.fedoraproject.org/packages/libbson>`_.
The package can be installed with:

.. code-block:: none

  $ yum install libbson

Installing from Source
----------------------

The following instructions are for UNIX-like systems such as GNU/Linux, FreeBSD, and Solaris. To build on Windows, see the instructions for :ref:`Building on Windows <installing_building_on_windows>`.

The most recent release of libbson is |release| and can be `downloaded here <https://github.com/mongodb/libbson/releases/download/|release|/libbson-|release|.tar.gz>`_. The following snippet will download and extract the current release of the driver.

.. parsed-literal::

  $ wget |release_download|
  $ tar -xzf libbson-|release|.tar.gz
  $ cd libbson-|release|/

Minimal dependencies are needed to build libbson. On UNIX-like systems, pthreads (the POSIX threading library) is required.

Make sure you have access to a :ref:`supported toolchain <installing_supported_platforms>` such as GCC, Clang, SolarisStudio, or MinGW. Optionally, ``pkg-config`` can be used if your system supports it to simplify locating proper compiler and linker arguments when compiling your program.

The following will configure for a typical 64-bit Linux system such as RedHat Enterprise Linux 6 or CentOS 6. Note that not all systems place 64-bit libraries in ``/usr/lib64``. Check your system to see what the convention is if you are building 64-bit versions of the library.

.. code-block:: none

  $ ./configure --prefix=/usr --libdir=/usr/lib64

For a list of all configure options, run ``./configure --help``.

If ``configure`` completed successfully, you'll see something like the following describing your build configuration.

.. parsed-literal::

  libbson |release| was configured with the following options:

  Build configuration:
    Enable debugging (slow)                          : no
    Enable extra alignment (required for 1.0 ABI)    : no
    Compile with debug symbols (slow)                : no
    Enable GCC build optimization                    : yes
    Code coverage support                            : no
    Cross Compiling                                  : no
    Big endian                                       : no
    Link Time Optimization (experimental)            : no

  Documentation:
    man                                              : no
    HTML                                             : no

We can now build libbson with the venerable ``make`` program.

.. code-block:: none

  $ make

.. note:

  You can optionally build code objects in parallel using the ``-j`` option to GNU make. Some implementations of ``make`` do not support this option, such as Sun's make on Solaris 10. To build in parallel on an 8 core machine, you might use:

  .. code-block:: none

    $ gmake -j8

To install the driver, we use ``make`` with the ``install`` target.

.. code-block:: none

  $ sudo make install

.. note:

  On systems that do not support the ``sudo`` command, we can use ``su -c 'make install'``.

.. _installing_building_on_windows:

Building on Windows
-------------------

Building on Windows requires Windows Vista or newer and Visual Studio 2010 or newer. Additionally, ``cmake`` is required to generate Visual Studio project files.

Let's start by generating Visual Studio project files for libbson. The following assumes we are compiling for 64-bit Windows using Visual Studio 2010 Express which can be freely downloaded from Microsoft.

.. parsed-literal::

  > cd libbson-|release|
  > cmake -G "Visual Studio 14 2015 Win64" \\
    "-DCMAKE_INSTALL_PREFIX=C:\\libbson"
  > msbuild.exe ALL_BUILD.vcxproj
  > msbuild.exe INSTALL.vcxproj

You should now see libbson installed in ``C:\libbson``

You can disable building the tests with:

.. code-block:: none

  > cmake -G "Visual Studio 14 2015 Win64" \
    "-DCMAKE_INSTALL_PREFIX=C:\libbson" \
    "-DENABLE_TESTS:BOOL=OFF"

