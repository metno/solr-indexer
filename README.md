# solrindexing branch before pr14

Useful tools and wrappers used for indexing MMD in SolR. This software is
developed for use in the context of Arctic Data Centre, supported through
projects SIOS KC and Norwegian Scientific Data Network.

## Usage

### Usage with directory and no thumbnails

```bash
indexdata -c etc/config.yml -d tests/data -n
```

### Logger object

* `SOLRINDEXER_LOGFILE` can be set to enable logging to file.
* `SOLRINDEXER_LOGLEVEL` can be set to change log level. See the Debugging section below.

## Installation

Install instructions for the solrindexer package.

### Install using python virtualenv

Make sure to have python3-venv installed.

```bash
sudo apt-get install python3-venv
```

Create a python virtualenv.

```bash
python -m venv testenv
```

Activate the virtualenv.

```bash
source testenv/bin/activate
```

Install the solrindexer package

```bash
pip install .
```

To enable support for thumbnail, additonal dependencies have to be installed.

```bash
pip install -r requirements-thumb.txt
```

### Install using conda

Solrindexing depends on cartopy, which cannot be installed in the regular "pip" way. On linux,
first install libproj and libgeos:

```bash
sudo apt-get install libproj-dev libgeos++-dev
```

To avoid problems with conflicting versions, we recommend using the Conda package manager. The below steps assume that Conda is installed.

Create a new environment:

```bash
conda env create -f environment.yml
```

Verify that the new environment is registered:

```bash
conda info --envs
```

Activate (use) the new environment:

```bash
conda activate solrindexing
```

Install the solrindexer package:

```text
pip install .
```

All dependencies should now be installed.

### Running python tests

To run python tests, install pytest dependency.

```bash
pip install pytest
```

Run the python tests with

```bash
python -m pytest -vv
```

### Running flake8 code syntax checker

To run the flake8 code syntax checker, install the flake8 dependency.

```bash
pip install flake8
```

Run the flake8 test

```bash
flake8 . --count --max-line-length=99 --ignore E221,E226,E228,E241 --show-source --statistics --exclude external
```
