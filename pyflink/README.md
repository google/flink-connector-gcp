### Build Your Own PyFlink Virtual environment

A virtual environment to use on both client and server can be created as
demonstrated below. It packs the current virtual environment to an archive file,
and it contains both Python interpreter and the dependencies.Use Python 3.10
[because venv-pack packs Python interpreter as a symbolic link.](https://github.com/jcrist/venv-pack/issues/5)

```bash
python -m venv pyflink_venv
source pyflink_venv/bin/activate
pip install "apache-flink==1.19.0" venv-pack
venv-pack -o pyflink_venv.tar.gz
```

Upload your virtual environment to GCS before using it on gcloud command.