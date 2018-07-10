FROM python:3

WORKDIR /opt/cadasta

# Setup User
RUN useradd -M -d $PWD user
RUN chown -R user.user $PWD

# Setup App
RUN apt-get update && apt-get install -y \
    libgdal-dev  \
    libgeos-dev
COPY --chown=user:user requirements-test.txt ./
RUN pip install --no-cache-dir -r requirements-test.txt
COPY --chown=user:user requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=user:user ./app ./app
COPY --chown=user:user ./tests ./tests
COPY --chown=user:user ./startworker ./runtests ./.coveragerc ./getversion ./

# Start App
USER user
CMD ./startworker -l INFO
