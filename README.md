# solrindexing

Useful tools and wrappers used for indexing MMD in SolR. This software is
developed for use in the context of Arctic Data Centre, supported through
projects SIOS KC and Norwegian Scientific Data Network.

## Usage indexdata

```text
usage: indexdata [-h] [-a] -c CFGFILE [-i INPUT_FILE] [-l LIST_FILE] [-d DIRECTORY] [-parent MARK_PARENT]
    [-t] [-n] [-m MAP_PROJECTION] [-t_layer THUMBNAIL_LAYER] [-t_style THUMBNAIL_STYLE]
    [-t_zl THUMBNAIL_ZOOM_LEVEL] [-ac [ADD_COASTLINES]] [-t_extent THUMBNAIL_EXTENT [THUMBNAIL_EXTENT ...]]

options:
  -h, --help            show this help message and exit
  -a, --always_commit   Specification of whether always commit or not to SolR
  -c CFGFILE, --cfg CFGFILE
                        Configuration file
  -i INPUT_FILE, --input_file INPUT_FILE
                        Individual file to be ingested.
  -l LIST_FILE, --list_file LIST_FILE
                        File with datasets to be ingested specified.
  -d DIRECTORY, --directory DIRECTORY
                        Directory to ingest
  -parent MARK_PARENT, --mark_parent MARK_PARENT
                        Enter metadata id of existing solr document to mark as parent
  -t, --thumbnail       Create and index thumbnail, do not update the main content.
  -n, --no_thumbnail    Do not index thumbnails (done automatically if WMS available).
  -m MAP_PROJECTION, --map_projection MAP_PROJECTION
                        Specify map projection for thumbnail (e.g. Mercator, PlateCarree, PolarStereographic).
  -t_layer THUMBNAIL_LAYER, --thumbnail_layer THUMBNAIL_LAYER
                        Specify wms_layer for thumbnail.
  -t_style THUMBNAIL_STYLE, --thumbnail_style THUMBNAIL_STYLE
                        Specify the style (colorscheme) for the thumbnail.
  -t_zl THUMBNAIL_ZOOM_LEVEL, --thumbnail_zoom_level THUMBNAIL_ZOOM_LEVEL
                        Specify the zoom level for the thumbnail.
  -ac [ADD_COASTLINES], --add_coastlines [ADD_COASTLINES]
                        Add coastlines too the thumbnail (True/False). Default True
  -t_extent THUMBNAIL_EXTENT [THUMBNAIL_EXTENT ...], --thumbnail_extent THUMBNAIL_EXTENT [THUMBNAIL_EXTENT ...]
                        Spatial extent of thumbnail in lat/lon degrees like "x0 x1 y0 y1"
```

### Example usage indexdata

```bash
indexdata -c etc/config.yml -d tests/data -n
```

will index all files in `tests/data`non-recursive

```bash
indexdata -c etc/config.yml -parent "c5ccf1dc-b223-4f44-984b-7edf95ba6012"
```

will mark the the dataseet with id `c5ccf1dc-b223-4f44-984b-7edf95ba6012`as a parent dataset.

## Usage bulkindexer

```text
usage: bulkindexer [-h] [-a] -c CFGFILE [-l LIST_FILE] [-d DIRECTORY] [-t] [-n] [-m MAP_PROJECTION]
    [-t_layer THUMBNAIL_LAYER] [-t_style THUMBNAIL_STYLE] [-t_zl THUMBNAIL_ZOOM_LEVEL]
    [-ac [ADD_COASTLINES]] [-t_extent THUMBNAIL_EXTENT [THUMBNAIL_EXTENT ...]]

options:
  -h, --help            show this help message and exit
  -a, --always_commit   Specification of whether always commit or not to SolR
  -c CFGFILE, --cfg CFGFILE
                        Configuration file
  -l LIST_FILE, --list_file LIST_FILE
                        File with datasets to be ingested specified.
  -d DIRECTORY, --directory DIRECTORY
                        Directory to ingest recursivly
  -t, --thumbnail       Create and index thumbnail, do not update the main content.
  -n, --no_thumbnail    Do not index thumbnails (done automatically if WMS available).
  -m MAP_PROJECTION, --map_projection MAP_PROJECTION
                        Specify map projection for thumbnail (e.g. Mercator, PlateCarree, PolarStereographic).
  -t_layer THUMBNAIL_LAYER, --thumbnail_layer THUMBNAIL_LAYER
                        Specify wms_layer for thumbnail.
  -t_style THUMBNAIL_STYLE, --thumbnail_style THUMBNAIL_STYLE
                        Specify the style (colorscheme) for the thumbnail.
  -t_zl THUMBNAIL_ZOOM_LEVEL, --thumbnail_zoom_level THUMBNAIL_ZOOM_LEVEL
                        Specify the zoom level for the thumbnail.
  -ac [ADD_COASTLINES], --add_coastlines [ADD_COASTLINES]
                        Add coastlines too the thumbnail (True/False). Default True
  -t_extent THUMBNAIL_EXTENT [THUMBNAIL_EXTENT ...], --thumbnail_extent THUMBNAIL_EXTENT [THUMBNAIL_EXTENT ...]
                        Spatial extent of thumbnail in lat/lon degrees like "x0 x1 y0 y1"
```

### Example usage bulkindexer with directory and no thumbnails

```bash
bulkindexer -c etc/config.yml -d tests/data -n
```

will index all MMD files in `tests/data` recursivly.

## Usage searchindex

```text
usage: searchindex [-h] -c CFGFILE -s STRING [-d] [-a]
```

### Example usage searchindex

```bash
searchindex -c etc/config.yml -s "<search_field>:<search_string>"
```

```bash
searchindex -c etc/config.yml -s "metadata_identifier:c5ccf1dc-b223-4f44-984b-7edf95ba6012"
```

```bash
searchindex -c etc/config.yml -s "sea -ice"
```

*If `<search_field>`is omitted, then `full_text` is the default search field.*

## Logging

* `SOLRINDEXER_LOGFILE` can be set to enable logging to file.
* `SOLRINDEXER_LOGLEVEL` can be set to change log level. See the Debugging section below.

```bash
indexdata -c etc/config.yml -i tests/data/mmdfile.xml 2> err.log
```

the above commoand will print info log to console and all other errors to `err.log`.

## Configuration file

The config file uses a toml/yaml syntax and should look like this:

```yaml
solrserver: http[s]://<solr_server_url>:<solr_port>/solr/
solrcore: <core_name/collection_name>
wms-thumbnail-projection: Mercator
wms-timeout: 480

# For solr basic authentication
auth-basic-username: <solr_auth_username>
auth-basic-password: <solr_password_username>
# If we do not want to expose solr credentials in this config file,
# we can use a .env file instead.
dotenv_path: <absolute_path_to_dotenv_file>

#For bulkindexer
batch-size: 250
workers: 2
threads: 2

#Commit to solr at end of execution True/False
#end-solr-commit: True

#Skip netCDF fe ature_type extraction if not set this is considered False
#skip-feature-type: True
#Override feature type (This will also skip the feature type)
#Can be used if we know the feature_type of all datasets indexed in this execution (list,dir)
#override-feature-type: profile

```

The `auth-basic-username` and `auth-basic-password`, can also be read from a `.env`-file. If a `.env`-file is in the current directory, it will be used, or a full path to the `.env`-file can be specified in the config file, with the `dotenv_path`-configuration key.

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

### Install on PPI

```bash
source /modules/rhel8/conda/install/etc/profile.d/conda.sh
conda activate production-08-2024
cd solr-indexer/
pip install .
```

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
