FROM python:3.9-buster

ENV DEBIAN_FRONTEND noninteractive

# Install distribution dependencies
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
    libsecp256k1-dev \
    wget  # To download IPFS

# Install IPFS
RUN wget https://dist.ipfs.io/go-ipfs/v0.8.0/go-ipfs_v0.8.0_linux-amd64.tar.gz
RUN tar -xvzf go-ipfs_v0.8.0_linux-amd64.tar.gz -C /opt/
RUN ln -s /opt/go-ipfs/ipfs /usr/local/bin/
RUN mkdir /var/lib/ipfs
# - User 'ipfs' to run IPFS -
#RUN useradd -s /bin/bash ipfs
#RUN chown -R ipfs:ipfs /var/lib/ipfs
RUN ipfs init

# Create unprivileged user accounts
RUN useradd -s /bin/bash --create-home source
RUN useradd -s /bin/bash --create-home user

# Install packages in a virtualenv to prevent installing them with root privileges (security)
RUN mkdir /opt/venv
RUN chown source:source /opt/venv
USER source
RUN python3 -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
WORKDIR /opt/aleph_nodestatus

# Copy the source code
COPY --chown=source:source . /opt/aleph_nodestatus
RUN python setup.py develop

# Run as unprivileged user (security)
USER user

WORKDIR /home/user
CMD "python /opt/aleph_nodestatus/src/aleph_nodestatus/scoring.py"
