# CAVATICA Dockerized Whistler

This was intended for use within CAVATICA. The biggest change from the original
design is that Whistle is embedded within the image itself and is designed to be
found therein as opposed to using the running environment.

## Steps to load into CAVATICA are as follows

### Build the image on your laptop (from your ncpi-whistler clone)

For the following steps, I'm providing my username, torstees, which is where the
image currently lives. If this is for our lab, that is probably best to keep,
however, for logins, be sure to use your actual user id.

```shell
# Build image
docker build --platform linux/amd64 -t ncpi-whistler:0.1.0 .

# Tag it
docker tag ncpi-whistler:0.1.0 pgc-images.sbgenomics.com/torstees/ncpi-whistler:0.1.0
```

### Log into the CAVATICA package repository

For this step, you will need to have generated an Authorization Token from
within the CAVATICA website.

```
# Log into package repo and push the image up for use inside the platform;
# For this, I've saved my Auth Token as an env var, CAVATICA_TOKEN and my
# CAVATICA username is the same as the one on my machine, so I can use the
# standard bash var, $USER. If your user id differs from your local machine,
# replace $USER with your id
echo "$CAVATICA_TOKEN" > docker login -u $USER --password-stdin pgc-images.sbgenomics.com

docker push pgc-images.sbgenomics.com/torstees/ncpi-whistler:0.1.0
```

### Push the CWL files up as tools

If you haven't installed the sevenbridges tool, you will need to do so using pip
and then push the scripts up using sbpack. The first argument is the name you'll
see inside the platform and the second is the script itself.

```shell
pip install sbpack

# Whistler-play
sbpack cavatica robert.carroll/include-dmc-transform-pilot/Whistler-play cavatica/ncpi-whistler-play.cwl

# Whistler-del
sbpack cavatica robert.carroll/include-dmc-transform-pilot/Whistler-del cavatica/ncpi-whistler-delete.cwl
```
