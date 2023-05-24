# How to deploy your python package?

If we have a lot of python modules (those are files .py), we can  collect them into single place <br>
called **package**. It gives us the opportunity to share a low-level code-components (i.e. Scrapers, Parsers etc.) within single<br>
environment. So we do not have to import files every time when we need to use the specific module, we have it **embedded** into our
environment.
## The structure of the package
The structure of the package goes like this.<br>
_Folder structure_
```shell script
- <package_name>
    - <module1.py>
    - <module2.py>
    ...
    - <module_x.py>
    -<subpackage_name>
      - <submodule1.py>
      - ...
      - <submodule_x.py>
    - __init__.py
```
Our python package can consist of another packages, which will contain coupled modules so for instance we can put <br>
Scraper module into Scraper subpackage, Parser module into Parsers etc.<br> 
In the *__init__.py* file we specify to which modules/subpackages (so python objects) the user has an access by using the following 
syntax:<br>
*Example of __init__.py*
```python
from <module_x.py> import <object_name>
from <subpackage_name>.<module_x.py> import <object_name>
```
## Setup.py for package
We structured our main package.<br>
So now we can create a special setup.py, which contains metadata about our package.<br>
It also tells the way how to build one.<br>
package<br>
*Folder structure*
```shell script
- <package_name> #Here we have the earlier defined package
- setup.py
```
*Example of setup.py* <br>
```python
from setuptools import setup,find_packages

setup(
    name='feeds', 
    version ='0.0.1',
    description = 'Package for maintaining low-level components', 
    packages=find_packages(), 
    package_data={'feeds':['configs/config.json']},
    install_requires=['pandas==1.0.3',
                      'psycopg2-binary==2.8.5',
                      'tqdm==4.45.0',
                      'selenium==3.141.0',
                      'beautifulsoup4==4.8.2',
                      'lxml==4.5.0',
                      'python-dateutil==2.8.1',
                      'requests==2.23.0']
)
```
Each argument means: <br>
**name** - specify the name of the package, it is does not have to be related to the name of the <package_name><br>
**version** - specify the version of the package - the format X.X.1 can indicate that the package is unstable<br>
**description** - set a description of the package for the end-user, tell them what it does. <br>
**packages** - instead of listing every subpackage manually, we can automate it by using find_packages(). Is also includes modules<br>
**package_data** - it includes non-python files for instance config files, sh scripts. 
The value must be a mapping from package name to a list of relative path names that should be copied into the package.  <br>
**install_requires** - specify packages, that our package depends on. 
<br>
## Creating a deployable package
There are two ways to create a package: <br> 
- source distribution
- built distribution

Source distribution is an archive, which has a setup.py, modules, data files etc. You can recompile everything on any platform <br>
To create a source distribution we go to the directory where setup.py is and type <br>
*Command to run*<br>
```shell script
python setup.py sdist
```
It will create a *.tar.gz for us, which is ready to install via pip on any machine <br>

Built distribution is like a binary package, it is specific to the platform (linux or windows) and 
python version. In linux RPM-based linux it's a RPM binary; in Windows it's a executable installer <br>
and in Debian it's a Debian package.
*Command to run* 
```shell script
python setup.py bdist
```

 
